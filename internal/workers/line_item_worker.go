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

	log.Printf("📦 Processing LineItem Event: type=%s, line_item_id=%d", evt.Event, evt.LineItemID)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	now := time.Now()
	version := uint64(now.UnixNano()) / 1000

	switch evt.Event {
	case "created", "updated":
		return w.syncLineItemUpsert(ctx, evt, version, now)
	case "deleted":
		return w.syncLineItemDelete(ctx, evt, version, now)
	default:
		return fmt.Errorf("unknown event type: %s", evt.Event)
	}
}

func (w *LineItemWorker) syncLineItemUpsert(ctx context.Context, evt models.LineItemDWHEvent, version uint64, ts time.Time) error {
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
		WHERE li.id = ? AND o.deleted_at IS NULL
	`

	var row liRow
	maxRetries := 3
	retryDelay := 100 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		if i > 0 {
			time.Sleep(retryDelay)
			retryDelay *= 2
		}

		err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.LineItemID).Scan(&row).Error
		if err == nil && row.LineItemID > 0 {
			break
		}

		if i == maxRetries-1 {
			return fmt.Errorf("failed to query line item %d after %d retries: %w", evt.LineItemID, maxRetries, err)
		}

		log.Printf("Retry %d/%d: Line item %d not found yet, retrying...", i+1, maxRetries, evt.LineItemID)
	}

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

	// Calculate delta
	var deltaRevenue float64
	var deltaSold int32
	var eventType string

	if evt.Event == "created" {
		deltaRevenue = totalRevenue
		deltaSold = int32(row.Quantity)
		eventType = "create"
	} else {
		// Update: delta = new - old
		oldMetrics, err := w.chClient.QueryOldLineItemMetrics(ctx, row.LineItemID)
		if err == nil {
			oldRevenue := oldMetrics["total_revenue"].(float64)
			oldSold := oldMetrics["total_sold"].(int32)
			deltaRevenue = totalRevenue - oldRevenue
			deltaSold = int32(row.Quantity) - oldSold
		} else {
			// If old value not found, treat as create
			deltaRevenue = totalRevenue
			deltaSold = int32(row.Quantity)
		}
		eventType = "update"
	}

	lineItemData := map[string]interface{}{
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

	if err := w.chClient.InsertLineItemDelta(ctx, lineItemData); err != nil {
		return fmt.Errorf("failed to insert line item delta: %w", err)
	}

	log.Printf("✓ LineItem event processed: line_item_id=%d, event=%s, delta_revenue=%.2f, delta_sold=%d", row.LineItemID, eventType, deltaRevenue, deltaSold)
	return nil
}

func (w *LineItemWorker) syncLineItemDelete(ctx context.Context, evt models.LineItemDWHEvent, version uint64, ts time.Time) error {
	// Query old line item metrics to calculate negative delta
	oldMetrics, err := w.chClient.QueryOldLineItemMetrics(ctx, evt.LineItemID)
	if err != nil {
		log.Printf("Line item not found in Fact_Line_Item for delete: %d", evt.LineItemID)
		return nil
	}

	// Query dimension keys from Postgres
	type liRow struct {
		OrderID    int64
		OrderDate  time.Time
		LocationID *int64
		SourceID   *int64
		StoreID    int64
		VariantID  int64
	}

	sql := `
		SELECT
			li.reference_id as order_id,
			DATE(o.created_at) as order_date,
			o.location_id,
			o.source_id,
			o.store_id,
			li.variant_id
		FROM line_items li
		INNER JOIN orders o ON o.id = li.reference_id AND li.reference_type = 'order'
		WHERE li.id = ?
	`

	var row liRow
	if err := w.pgClient.DB().WithContext(ctx).Raw(sql, evt.LineItemID).Scan(&row).Error; err != nil {
		return fmt.Errorf("failed to query line item keys: %w", err)
	}

	dateKey := row.OrderDate.Format("02012006")
	locID := int64(0)
	if row.LocationID != nil {
		locID = *row.LocationID
	}
	sourceID := int64(0)
	if row.SourceID != nil {
		sourceID = *row.SourceID
	}

	// Insert negative delta
	lineItemData := map[string]interface{}{
		"line_item_id":  evt.LineItemID,
		"order_id":      row.OrderID,
		"date_key":      dateKey,
		"location_key":  locID,
		"source_key":    sourceID,
		"store_key":     row.StoreID,
		"variant_key":   row.VariantID,
		"delta_revenue": -oldMetrics["total_revenue"].(float64),
		"delta_sold":    -oldMetrics["total_sold"].(int32),
		"event_type":    "cancel",
		"event_time":    ts,
	}

	if err := w.chClient.InsertLineItemDelta(ctx, lineItemData); err != nil {
		return fmt.Errorf("failed to insert line item delete delta: %w", err)
	}

	log.Printf("✓ LineItem deleted: line_item_id=%d", evt.LineItemID)
	return nil
}
