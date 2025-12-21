package models

import "time"

// OrderDWHEvent is the message payload from RabbitMQ for order events
type OrderDWHEvent struct {
	Event   string `json:"event"`    // created | updated | cancelled
	OrderID int64  `json:"order_id"` // ID in Postgres
	StoreID int64  `json:"store_id"` // store scope
}

// LineItemDWHEvent is the message payload from RabbitMQ for line item events
type LineItemDWHEvent struct {
	Event      string `json:"event"`        // created | updated | deleted
	LineItemID int64  `json:"line_item_id"` // ID in Postgres
	OrderID    int64  `json:"order_id"`     // ID in Postgres
	StoreID    int64  `json:"store_id"`     // store scope
}

// OrderData represents the aggregated order data from Postgres
type OrderData struct {
	OrderID          int64
	CustomerID       *int64
	LocationID       *int64
	SourceID         *int64
	StoreID          int64
	OrderDate        time.Time
	TotalShippingFee float64
	TotalLineFee     float64
	TotalRevenue     float64
}

// LineItemData represents the line item data from Postgres
type LineItemData struct {
	LineItemID int64
	OrderID    int64
	Price      float64
	Quantity   int64
}
