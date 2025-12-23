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

type LineItemWorker struct {
	consumer  *rabbitmq.Consumer
	chClient  *clickhouse.Client
	pgClient  *postgres.Client
	queueName string
}

func NewLineItemWorker(consumer *rabbitmq.Consumer, chClient *clickhouse.Client, pgClient *postgres.Client, queueName string) *LineItemWorker {
	return &LineItemWorker{
		consumer:  consumer,
		chClient:  chClient,
		pgClient:  pgClient,
		queueName: queueName,
	}
}

func (w *LineItemWorker) Start() error {
	log.Printf("🚀 Starting LineItem Worker for queue: %s", w.queueName)
	return w.consumer.ConsumeQueue(w.queueName, w.handleMessage)
}

func (w *LineItemWorker) handleMessage(body []byte) error {
	var evt models.LineItemDWHEvent
	if err := json.Unmarshal(body, &evt); err != nil {
		return fmt.Errorf("failed to unmarshal line item event: %w", err)
	}

	log.Printf("📦 Processing LineItem Event: type=%s, order_id=%d", evt.Event, evt.OrderID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now()
	version := uint64(now.UnixNano()) / 1000

	switch evt.Event {
	case "created", "updated":
		return w.syncAllLineItemsForOrder(ctx, evt, version, now)
	default:
		return fmt.Errorf("unknown event type: %s", evt.Event)
	}
}

func (w *LineItemWorker) syncAllLineItemsForOrder(ctx context.Context, evt models.LineItemDWHEvent, version uint64, ts time.Time) error {
	// Query all current line items from Postgres for this order
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

	var currentLineItems []liRow
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.OrderID).Scan(&currentLineItems).Error
		if err == nil {
			break
		}

		if i == maxRetries-1 {
			return fmt.Errorf("failed to query line items for order %d after %d retries: %w", evt.OrderID, maxRetries, err)
		}
	}

	if len(currentLineItems) == 0 {
		log.Printf("No line items found for order %d", evt.OrderID)
	}

	// Get all existing line items from ClickHouse for this order
	oldLineItems, err := w.chClient.QueryAllLineItemsForOrder(ctx, evt.OrderID)
	if err != nil {
		log.Printf("Failed to query old line items for order %d: %v, treating as new order", evt.OrderID, err)
		oldLineItems = make(map[int64]map[string]interface{})
	}

	// Build map of current line items for quick lookup
	currentLineItemMap := make(map[int64]liRow)
	for _, li := range currentLineItems {
		currentLineItemMap[li.LineItemID] = li
	}

	// Process deletions: line items in ClickHouse but not in current Postgres data
	for oldLineItemID, oldMetrics := range oldLineItems {
		if _, exists := currentLineItemMap[oldLineItemID]; !exists {
			// Line item was deleted, insert negative delta
			if err := w.insertDeleteDelta(ctx, oldLineItemID, evt.OrderID, oldMetrics, version, ts); err != nil {
				log.Printf("Failed to insert delete delta for line_item %d: %v", oldLineItemID, err)
			}

			// Mark as deleted in Fact_Line_Item
			if err := w.chClient.MarkLineItemAsDeleted(ctx, oldLineItemID, version, ts); err != nil {
				log.Printf("Failed to mark line_item %d as deleted: %v", oldLineItemID, err)
			}
		}
	}

	// Process updates and creates
	for _, row := range currentLineItems {
		dateKey := row.OrderDate.Format("02012006")
		locID := int64(0)
		if row.LocationID != nil {
			locID = *row.LocationID
		}
		sourceID := int64(0)
		if row.SourceID != nil {
			sourceID = *row.SourceID
		}

		totalRevenue := row.Price * float64(row.Quantity)

		var deltaRevenue float64
		var deltaSold int32
		var eventType string

		if oldMetrics, exists := oldLineItems[row.LineItemID]; exists {
			// Update: calculate delta
			oldRevenue := oldMetrics["total_revenue"].(float64)
			oldSold := int32(oldMetrics["total_sold"].(uint32))
			deltaRevenue = totalRevenue - oldRevenue
			deltaSold = int32(row.Quantity) - oldSold
			eventType = "update"
		} else {
			// Create: all values are delta
			deltaRevenue = totalRevenue
			deltaSold = int32(row.Quantity)
			eventType = "create"
		}

		// Insert delta
		lineItemDeltaData := map[string]interface{}{
			"line_item_id":  row.LineItemID,
			"order_id":      row.OrderID,
			"date_key":      dateKey,
			"location_key":  locID,
			"source_key":    sourceID,
			"store_key":     row.StoreID,
			"variant_key":   row.VariantID,
			"delta_revenue": deltaRevenue,
			"delta_sold":    deltaSold,
			"event_type":    eventType,
			"event_time":    ts,
		}

		if err := w.chClient.InsertLineItemDelta(ctx, lineItemDeltaData); err != nil {
			log.Printf("Failed to insert line item delta for %d: %v", row.LineItemID, err)
			continue
		}

		// Insert/Update Fact_Line_Item with current values
		lineItemFactData := map[string]interface{}{
			"line_item_id":  row.LineItemID,
			"order_id":      row.OrderID,
			"date_key":      dateKey,
			"location_key":  locID,
			"source_key":    sourceID,
			"store_key":     row.StoreID,
			"variant_key":   row.VariantID,
			"total_revenue": totalRevenue,
			"total_sold":    uint32(row.Quantity),
			"is_deleted":    uint8(0),
			"_version":      version,
			"_updated_at":   ts,
		}

		if err := w.chClient.InsertLineItemFact(ctx, lineItemFactData); err != nil {
			log.Printf("Failed to insert line item fact for %d: %v", row.LineItemID, err)
			continue
		}

		log.Printf("✓ LineItem processed: line_item_id=%d, event=%s, delta_revenue=%.2f, delta_sold=%d", row.LineItemID, eventType, deltaRevenue, deltaSold)
	}

	log.Printf("✓ Synced %d line items for order %d (checked %d old items)", len(currentLineItems), evt.OrderID, len(oldLineItems))
	return nil
}

func (w *LineItemWorker) insertDeleteDelta(ctx context.Context, lineItemID int64, orderID int64, oldMetrics map[string]interface{}, version uint64, ts time.Time) error {
	lineItemData := map[string]interface{}{
		"line_item_id":  lineItemID,
		"order_id":      orderID,
		"date_key":      oldMetrics["date_key"].(string),
		"location_key":  oldMetrics["location_key"].(int32),
		"source_key":    oldMetrics["source_key"].(int32),
		"store_key":     oldMetrics["store_key"].(int32),
		"variant_key":   oldMetrics["variant_key"].(int32),
		"delta_revenue": -oldMetrics["total_revenue"].(float64),
		"delta_sold":    -int32(oldMetrics["total_sold"].(uint32)),
		"event_type":    "cancel",
		"event_time":    ts,
	}

	if err := w.chClient.InsertLineItemDelta(ctx, lineItemData); err != nil {
		return fmt.Errorf("failed to insert line item delete delta: %w", err)
	}

	log.Printf("✓ LineItem deleted: line_item_id=%d", lineItemID)
	return nil
}
