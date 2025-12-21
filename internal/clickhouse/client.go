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
	query := fmt.Sprintf(`
		SELECT 
			total_revenue,
			total_shipping_fee,
			total_line_item_fee,
			total_count
		FROM %s.Fact_Order
		WHERE order_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database)

	row := c.conn.QueryRow(ctx, query, orderID)

	var totalRevenue, totalShippingFee, totalLineItemFee float64
	var totalCount int32

	if err := row.Scan(&totalRevenue, &totalShippingFee, &totalLineItemFee, &totalCount); err != nil {
		return nil, err
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
	query := fmt.Sprintf(`
		SELECT 
			total_revenue,
			total_sold
		FROM %s.Fact_Line_Item
		WHERE line_item_id = ?
		ORDER BY _version DESC
		LIMIT 1
	`, c.database)

	row := c.conn.QueryRow(ctx, query, lineItemID)

	var totalRevenue float64
	var totalSold int32

	if err := row.Scan(&totalRevenue, &totalSold); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"total_revenue": totalRevenue,
		"total_sold":    totalSold,
	}, nil
}

func (c *Client) Conn() driver.Conn {
	return c.conn
}
