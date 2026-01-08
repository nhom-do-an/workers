package clickhouse

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"workers/config"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Client struct {
	conn     driver.Conn
	database string
}

func NewClient(cfg config.ClickHouseConfig) (*Client, error) {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		DialTimeout:  time.Second * 30,
	}

	// Only use TLS if explicitly needed (when using HTTPS port 8443)
	// For native protocol on port 9000, don't use TLS by default
	if cfg.Port == 8443 {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	conn, err := clickhouse.Open(opts)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return &Client{
		conn:     conn,
		database: cfg.Database,
	}, nil
}

func (c *Client) Close() error {
	return c.conn.Close()
}

// InsertOrderDelta inserts order delta into ClickHouse
func (c *Client) InsertOrderDelta(ctx context.Context, data map[string]interface{}) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Order_Delta (
			order_id, date_key, location_key, customer_key, source_key, store_key,
			delta_revenue, delta_shipping_fee, delta_line_item_fee, delta_orders,
			event_type, event_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.database)

	if err := c.conn.Exec(ctx, query,
		data["order_id"],
		data["date_key"],
		data["location_key"],
		data["customer_key"],
		data["source_key"],
		data["store_key"],
		data["delta_revenue"],
		data["delta_shipping_fee"],
		data["delta_line_item_fee"],
		data["delta_orders"],
		data["event_type"],
		data["event_time"],
	); err != nil {

		return err
	}
	// Also insert/update Fact_Order for reference (ReplacingMergeTree)
	insertOrderSQL := fmt.Sprintf(`
		INSERT INTO %s.Fact_Order (
			order_id,
			date_key,
			location_key,
			customer_key,
			source_key,
			store_key,
			total_shipping_fee,
			total_line_item_fee,
			total_revenue,
			total_count,
			is_deleted,
			_version,
			_updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.database)

	if err := c.conn.Exec(ctx, insertOrderSQL,
		data["order_id"],
		data["date_key"],
		data["location_key"],
		data["customer_key"],
		data["source_key"],
		data["store_key"],
		data["total_shipping_fee"],   // 7. total_shipping_fee
		data["total_line_item_fee"],  // 8. total_line_item_fee
		data["total_revenue"],        // 9. total_revenue
		1,                            // 10. total_count
		uint8(0),                     // 11. is_deleted
		data["_version"],             // 12. _version
		data["event_time"],           // 13. _updated_at
	); err != nil {
		return err
	}
	return nil
}

// InsertOrderDeltaOnly inserts ONLY into Fact_Order_Delta (for cancel events)
func (c *Client) InsertOrderDeltaOnly(ctx context.Context, data map[string]interface{}) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Order_Delta (
			order_id, date_key, location_key, customer_key, source_key, store_key,
			delta_revenue, delta_shipping_fee, delta_line_item_fee, delta_orders,
			event_type, event_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.database)

	return c.conn.Exec(ctx, query,
		data["order_id"],
		data["date_key"],
		data["location_key"],
		data["customer_key"],
		data["source_key"],
		data["store_key"],
		data["delta_revenue"],
		data["delta_shipping_fee"],
		data["delta_line_item_fee"],
		data["delta_orders"],
		data["event_type"],
		data["event_time"],
	)
}

// MarkOrderAsDeleted marks an order as deleted in Fact_Order (for cancel events)
func (c *Client) MarkOrderAsDeleted(ctx context.Context, orderID int64, version uint64, ts time.Time) error {
	// Don't use FINAL - query the latest version directly
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Order (
			order_id,
			date_key,
			location_key,
			customer_key,
			source_key,
			store_key,
			total_shipping_fee,
			total_line_item_fee,
			total_revenue,
			total_count,
			is_deleted,
			_version,
			_updated_at
		)
		SELECT
			order_id,
			date_key,
			location_key,
			customer_key,
			source_key,
			store_key,
			total_shipping_fee,
			total_line_item_fee,
			total_revenue,
			0 as total_count,
			1 as is_deleted,
			? as _version,
			? as _updated_at
		FROM %s.Fact_Order
		WHERE order_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database, c.database)

	return c.conn.Exec(ctx, query, version, ts, orderID)
}

// InsertLineItemDelta inserts line item delta into ClickHouse
func (c *Client) InsertLineItemDelta(ctx context.Context, data map[string]interface{}) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Line_Item_Delta (
			line_item_id, order_id, date_key, location_key, source_key, store_key,
			variant_key, delta_revenue, delta_sold,
			event_type, event_time
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.database)

	return c.conn.Exec(ctx, query,
		data["line_item_id"],
		data["order_id"],
		data["date_key"],
		data["location_key"],
		data["source_key"],
		data["store_key"],
		data["variant_key"],
		data["delta_revenue"],
		data["delta_sold"],
		data["event_type"],
		data["event_time"],
	)
}

// QueryOldOrderMetrics queries old order metrics from ClickHouse for delta calculation
func (c *Client) QueryOldOrderMetrics(ctx context.Context, orderID int64) (map[string]interface{}, error) {
	// Don't use FINAL - it may not return recently inserted data
	// Query directly and filter by is_deleted after getting the latest version
	query := fmt.Sprintf(`
		SELECT
			total_revenue,
			total_shipping_fee,
			total_line_item_fee,
			total_count,
			is_deleted
		FROM %s.Fact_Order
		WHERE order_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database)

	row := c.conn.QueryRow(ctx, query, orderID)

	var totalRevenue, totalShippingFee, totalLineItemFee float64
	var totalCount uint32
	var isDeleted uint8

	if err := row.Scan(&totalRevenue, &totalShippingFee, &totalLineItemFee, &totalCount, &isDeleted); err != nil {
		return nil, err
	}

	// Check if the record is deleted
	if isDeleted == 1 {
		return nil, fmt.Errorf("order %d is deleted", orderID)
	}

	return map[string]interface{}{
		"total_revenue":       totalRevenue,
		"total_shipping_fee":  totalShippingFee,
		"total_line_item_fee": totalLineItemFee,
		"total_count":         totalCount,
	}, nil
}

// QueryOldLineItemMetrics queries old line item metrics from ClickHouse for delta calculation
func (c *Client) QueryOldLineItemMetrics(ctx context.Context, lineItemID int64) (map[string]interface{}, error) {
	// Don't use FINAL - it may not return recently inserted data
	query := fmt.Sprintf(`
		SELECT
			total_revenue,
			total_sold,
			is_deleted
		FROM %s.Fact_Line_Item
		WHERE line_item_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database)

	row := c.conn.QueryRow(ctx, query, lineItemID)

	var totalRevenue float64
	var totalSold uint32
	var isDeleted uint8

	if err := row.Scan(&totalRevenue, &totalSold, &isDeleted); err != nil {
		return nil, err
	}

	// Check if the record is deleted
	if isDeleted == 1 {
		return nil, fmt.Errorf("line_item %d is deleted", lineItemID)
	}

	return map[string]interface{}{
		"total_revenue": totalRevenue,
		"total_sold":    totalSold,
	}, nil
}

// QueryAllLineItemsForOrder queries all line items for an order from ClickHouse
func (c *Client) QueryAllLineItemsForOrder(ctx context.Context, orderID int64) (map[int64]map[string]interface{}, error) {
	// Don't use FINAL - use argMax to get the latest version for each line_item_id
	// Use subquery to filter by is_deleted after aggregation
	query := fmt.Sprintf(`
		SELECT
			line_item_id,
			date_key,
			location_key,
			source_key,
			store_key,
			variant_key,
			total_revenue,
			total_sold
		FROM (
			SELECT
				line_item_id,
				argMax(date_key, _version) as date_key,
				argMax(location_key, _version) as location_key,
				argMax(source_key, _version) as source_key,
				argMax(store_key, _version) as store_key,
				argMax(variant_key, _version) as variant_key,
				argMax(total_revenue, _version) as total_revenue,
				argMax(total_sold, _version) as total_sold,
				argMax(is_deleted, _version) as is_deleted
			FROM %s.Fact_Line_Item
			WHERE order_id = ?
			GROUP BY line_item_id
		)
		WHERE is_deleted = 0
	`, c.database)

	rows, err := c.conn.Query(ctx, query, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]map[string]interface{})
	for rows.Next() {
		var lineItemID int64
		var dateKey string
		var locationKey, sourceKey, storeKey, variantKey int32
		var totalRevenue float64
		var totalSold uint32

		if err := rows.Scan(&lineItemID, &dateKey, &locationKey, &sourceKey, &storeKey, &variantKey, &totalRevenue, &totalSold); err != nil {
			continue
		}

		result[lineItemID] = map[string]interface{}{
			"date_key":      dateKey,
			"location_key":  locationKey,
			"source_key":    sourceKey,
			"store_key":     storeKey,
			"variant_key":   variantKey,
			"total_revenue": totalRevenue,
			"total_sold":    totalSold,
		}
	}

	return result, nil
}

// MarkLineItemAsDeleted marks a line item as deleted in Fact_Line_Item
func (c *Client) MarkLineItemAsDeleted(ctx context.Context, lineItemID int64, version uint64, ts time.Time) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Line_Item (
			line_item_id,
			order_id,
			date_key,
			location_key,
			source_key,
			store_key,
			variant_key,
			total_revenue,
			total_sold,
			is_deleted,
			_version,
			_updated_at
		)
		SELECT 
			line_item_id,
			order_id,
			date_key,
			location_key,
			source_key,
			store_key,
			variant_key,
			total_revenue,
			total_sold,
			1 as is_deleted,
			? as _version,
			? as _updated_at
		FROM %s.Fact_Line_Item
		WHERE line_item_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database, c.database)

	return c.conn.Exec(ctx, query, version, ts, lineItemID)
}

// InsertLineItemFact inserts or updates line item in Fact_Line_Item
func (c *Client) InsertLineItemFact(ctx context.Context, data map[string]interface{}) error {
	query := fmt.Sprintf(`
		INSERT INTO %s.Fact_Line_Item (
			line_item_id,
			order_id,
			date_key,
			location_key,
			source_key,
			store_key,
			variant_key,
			total_revenue,
			total_sold,
			is_deleted,
			_version,
			_updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, c.database)

	return c.conn.Exec(ctx, query,
		data["line_item_id"],
		data["order_id"],
		data["date_key"],
		data["location_key"],
		data["source_key"],
		data["store_key"],
		data["variant_key"],
		data["total_revenue"],
		data["total_sold"],
		data["is_deleted"],
		data["_version"],
		data["_updated_at"],
	)
}

func (c *Client) Conn() driver.Conn {
	return c.conn
}
