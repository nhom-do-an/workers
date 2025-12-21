package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"workers/internal/clickhouse"
	"workers/internal/postgres"
	"workers/internal/rabbitmq"
	"workers/models"
)

type OrderWorker struct {
	consumer  *rabbitmq.Consumer
	chClient  *clickhouse.Client
	pgClient  *postgres.Client
	queueName string
}

func NewOrderWorker(consumer *rabbitmq.Consumer, chClient *clickhouse.Client, pgClient *postgres.Client, queueName string) *OrderWorker {
	return &OrderWorker{
		consumer:  consumer,
		chClient:  chClient,
		pgClient:  pgClient,
		queueName: queueName,
	}
}

func (w *OrderWorker) Start() error {
	log.Printf("🚀 Starting Order Worker for queue: %s", w.queueName)
	return w.consumer.ConsumeQueue(w.queueName, w.handleMessage)
}

func (w *OrderWorker) handleMessage(body []byte) error {
	var evt models.OrderDWHEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return fmt.Errorf("failed to unmarshal order event: %w", err)
	}

	log.Printf("📦 Processing Order Event: type=%s, order_id=%d", evt.Event, evt.OrderID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now()
	version := uint64(now.UnixNano()) / 1000

	switch evt.Event {
	case "created", "updated":
		return w.syncOrderUpsert(ctx, evt, version, now)
	case "cancelled":
		return w.syncOrderCancel(ctx, evt, version, now)
	default:
		return fmt.Errorf("unknown event type: %s", evt.Event)
	}
}

func (w *OrderWorker) syncOrderUpsert(ctx context.Context, evt models.OrderDWHEvent, version uint64, ts time.Time) error {
	// Query order data from Postgres with retries
	var row models.OrderData
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	sql := `
		SELECT 
			o.id as order_id,
			o.customer_id,
			o.location_id,
			o.source_id,
			o.store_id,
			DATE(o.created_at) as order_date,
			COALESCE((
				SELECT SUM(price)
				FROM shipping_lines 
				WHERE order_id = o.id
			), 0)::double precision as total_shipping_fee,
			COALESCE((
				SELECT SUM(amount)
				FROM transactions 
				WHERE reference_id = o.id 
				AND reference_type = 'order'
				AND status = 2
				AND kind = 'sale'
			), 0)::double precision as total_revenue
		FROM orders o
		WHERE o.id = ? AND o.deleted_at IS NULL
	`

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.OrderID).Scan(&row).Error
		if err == nil && row.OrderID > 0 {
			break
		}

		if i == maxRetries-1 {
			return fmt.Errorf("failed to query order %d after %d retries: %w", evt.OrderID, maxRetries, err)
		}

		log.Printf("Retry %d/%d: Order %d not found yet, retrying...", i+1, maxRetries, evt.OrderID)
	}

	// Query line items to calculate total_line_item_fee
	lineItemsSQL := `
		SELECT li.price, li.quantity
		FROM line_items li
		WHERE li.reference_id = ? AND li.reference_type = 'order'
	`

	var lineItemRows []models.LineItemData
	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		err := w.pgClient.DB().WithContext(ctx).Raw(lineItemsSQL, evt.OrderID).Scan(&lineItemRows).Error
		if err == nil {
			break
		}

		if i == maxRetries-1 {
			log.Printf("Failed to query line items for order %d, using 0", evt.OrderID)
		}
	}

	// Calculate total_line_item_fee
	totalLineItemFee := 0.0
	for _, li := range lineItemRows {
		totalLineItemFee += li.Price * float64(li.Quantity)
	}
	row.TotalLineFee = totalLineItemFee

	log.Printf("Order %d: shipping_fee=%.2f, line_item_fee=%.2f (from %d items), revenue=%.2f",
		row.OrderID, row.TotalShippingFee, row.TotalLineFee, len(lineItemRows), row.TotalRevenue)

	dateKey := row.OrderDate.Format("02012006") // ddMMYYYY

	locID := int64(0)
	if row.LocationID != nil {
		locID = *row.LocationID
	}
	custID := int64(0)
	if row.CustomerID != nil {
		custID = *row.CustomerID
	}
	sourceID := int64(0)
	if row.SourceID != nil {
		sourceID = *row.SourceID
	}

	// Calculate delta
	var deltaRevenue, deltaShippingFee, deltaLineItemFee float64
	var deltaOrders int32
	var eventType string

	if evt.Event == "created" {
		deltaRevenue = row.TotalRevenue
		deltaShippingFee = row.TotalShippingFee
		deltaLineItemFee = row.TotalLineFee
		deltaOrders = 1
		eventType = "create"
	} else {
		// Update: delta = new - old
		oldMetrics, err := w.chClient.QueryOldOrderMetrics(ctx, row.OrderID)
		if err == nil {
			deltaRevenue = row.TotalRevenue - oldMetrics["total_revenue"].(float64)
			deltaShippingFee = row.TotalShippingFee - oldMetrics["total_shipping_fee"].(float64)
			deltaLineItemFee = row.TotalLineFee - oldMetrics["total_line_item_fee"].(float64)
			deltaOrders = 0 // usually 0 for update
		} else {
			// If old value not found, treat as create
			deltaRevenue = row.TotalRevenue
			deltaShippingFee = row.TotalShippingFee
			deltaLineItemFee = row.TotalLineFee
			deltaOrders = 1
		}
		eventType = "update"
	}

	// Insert into Fact_Order_Delta
	deltaData := map[string]interface{}{
		"order_id":            row.OrderID,
		"date_key":            dateKey,
		"location_key":        locID,
		"customer_key":        custID,
		"source_key":          sourceID,
		"store_key":           row.StoreID,
		"delta_revenue":       deltaRevenue,
		"delta_shipping_fee":  deltaShippingFee,
		"delta_line_item_fee": deltaLineItemFee,
		"delta_orders":        deltaOrders,
		"event_type":          eventType,
		"event_time":          ts,
	}

	if err := w.chClient.InsertOrderDelta(ctx, deltaData); err != nil {
		return fmt.Errorf("failed to insert order delta: %w", err)
	}

	log.Printf("✓ Order event processed: order_id=%d, event=%s, delta_revenue=%.2f", row.OrderID, eventType, deltaRevenue)

	// Sync line items for this order
	return w.syncLineItemsForOrder(ctx, evt)
}

func (w *OrderWorker) syncLineItemsForOrder(ctx context.Context, evt models.OrderDWHEvent) error {
	type liRow struct {
		LineItemID int64
		OrderID    int64
		VariantID  int64
		Quantity   int64
		Price      float64
		LocationID *int64
		SourceID   *int64
		StoreID    int64
		OrderDate  time.Time
	}

	sql := `
		SELECT
			li.id as line_item_id,
			li.reference_id as order_id,
			li.variant_id,
			li.quantity,
			li.price,
			o.location_id,
			o.source_id,
			o.store_id,
			DATE(o.created_at) as order_date
		FROM line_items li
		INNER JOIN orders o ON o.id = li.reference_id AND li.reference_type = 'order'
		WHERE o.id = ? AND o.deleted_at IS NULL
	`

	var rows []liRow
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.OrderID).Scan(&rows).Error
		if err == nil {
			break
		}

		if i == maxRetries-1 {
			return fmt.Errorf("failed to query line items for order %d after %d retries: %w", evt.OrderID, maxRetries, err)
		}
	}

	if len(rows) == 0 {
		log.Printf("No line items found for order %d", evt.OrderID)
		return nil
	}

	// Insert line items into ClickHouse using batch
	for _, r := range rows {
		dateKey := r.OrderDate.Format("02012006")
		locID := int64(0)
		if r.LocationID != nil {
			locID = *r.LocationID
		}
		sourceID := int64(0)
		if r.SourceID != nil {
			sourceID = *r.SourceID
		}

		totalRevenue := r.Price * float64(r.Quantity)

		// Calculate delta based on event type
		var deltaRevenue float64
		var deltaSold int32
		var eventTypeStr string

		if evt.Event == "created" {
			deltaRevenue = totalRevenue
			deltaSold = int32(r.Quantity)
			eventTypeStr = "create"
		} else if evt.Event == "cancelled" {
			deltaRevenue = -totalRevenue
			deltaSold = -int32(r.Quantity)
			eventTypeStr = "cancel"
		} else {
			// For update, we may need to query old values
			// For simplicity, treat as create for now
			deltaRevenue = totalRevenue
			deltaSold = int32(r.Quantity)
			eventTypeStr = "update"
		}

		lineItemData := map[string]interface{}{
			"line_item_id":  r.LineItemID,
			"order_id":      r.OrderID,
			"date_key":      dateKey,
			"location_key":  locID,
			"source_key":    sourceID,
			"store_key":     r.StoreID,
			"variant_key":   r.VariantID,
			"delta_revenue": deltaRevenue,
			"delta_sold":    deltaSold,
			"event_type":    eventTypeStr,
			"event_time":    ts,
		}

		if err := w.chClient.InsertLineItemDelta(ctx, lineItemData); err != nil {
			log.Printf("Failed to insert line item %d: %v", r.LineItemID, err)
			// Continue with other line items
		}
	}

	log.Printf("✓ Synced %d line items for order %d", len(rows), evt.OrderID)
	return nil
}

func (w *OrderWorker) syncOrderCancel(ctx context.Context, evt models.OrderDWHEvent, version uint64, ts time.Time) error {
	// Query current order values to calculate negative delta
	oldMetrics, err := w.chClient.QueryOldOrderMetrics(ctx, evt.OrderID)
	if err != nil {
		log.Printf("Order not found in Fact_Order for cancel: %d", evt.OrderID)
		return nil
	}

	// We need to get dimensional keys from the old record
	// For simplicity, we'll query from Postgres
	type orderKeys struct {
		DateKey    time.Time
		LocationID *int64
		CustomerID *int64
		SourceID   *int64
	}

	sql := `
		SELECT 
			DATE(o.created_at) as date_key,
			o.location_id,
			o.customer_id,
			o.source_id
		FROM orders o
		WHERE o.id = ?
	`

	var keys orderKeys
	if err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.OrderID).Scan(&keys).Error; err != nil {
		return fmt.Errorf("failed to query order keys: %w", err)
	}

	dateKey := keys.DateKey.Format("02012006")
	locID := int64(0)
	if keys.LocationID != nil {
		locID = *keys.LocationID
	}
	custID := int64(0)
	if keys.CustomerID != nil {
		custID = *keys.CustomerID
	}
	sourceID := int64(0)
	if keys.SourceID != nil {
		sourceID = *keys.SourceID
	}

	// Insert negative delta
	deltaData := map[string]interface{}{
		"order_id":            evt.OrderID,
		"date_key":            dateKey,
		"location_key":        locID,
		"customer_key":        custID,
		"source_key":          sourceID,
		"store_key":           evt.StoreID,
		"delta_revenue":       -oldMetrics["total_revenue"].(float64),
		"delta_shipping_fee":  -oldMetrics["total_shipping_fee"].(float64),
		"delta_line_item_fee": -oldMetrics["total_line_item_fee"].(float64),
		"delta_orders":        -int32(oldMetrics["total_count"].(int32)),
		"event_type":          "cancel",
		"event_time":          ts,
	}

	if err := w.chClient.InsertOrderDelta(ctx, deltaData); err != nil {
		return fmt.Errorf("failed to insert cancel delta: %w", err)
	}

	log.Printf("✓ Order cancelled: order_id=%d", evt.OrderID)
	return nil
}
