package rabbitmq

import (
	"encoding/json"
	"fmt"
	"log"

	"workers/config"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  config.RabbitMQConfig
}

func NewConsumer(cfg config.RabbitMQConfig) (*Consumer, error) {
	conn, err := amqp.Dial(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	// Set prefetch count
	if err := channel.Qos(cfg.PrefetchCount, 0, false); err != nil {
		channel.Close()
		conn.Close()
		return nil, fmt.Errorf("failed to set QoS: %w", err)
	}

	return &Consumer{
		conn:    conn,
		channel: channel,
		config:  cfg,
	}, nil
}

func (c *Consumer) Close() {
	if c.channel != nil {
		c.channel.Close()
	}
	if c.conn != nil {
		c.conn.Close()
	}
}

func (c *Consumer) ConsumeQueue(queueName string, handler func([]byte) error) error {
	// Declare queue (idempotent)
	_, err := c.channel.QueueDeclare(
		queueName,
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return fmt.Errorf("failed to declare queue: %w", err)
	}

	msgs, err := c.channel.Consume(
		queueName,
		"",    // consumer tag
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		return fmt.Errorf("failed to register consumer: %w", err)
	}

	log.Printf("✓ Started consuming from queue: %s", queueName)

	for msg := range msgs {
		if err := c.processMessage(msg, handler); err != nil {
			log.Printf("✗ Error processing message: %v", err)
			// Reject and requeue
			msg.Nack(false, true)
		} else {
			// Acknowledge
			msg.Ack(false)
		}
	}

	return nil
}

func (c *Consumer) processMessage(msg amqp.Delivery, handler func([]byte) error) error {
	// Call handler
	if err := handler(msg.Body); err != nil {
		return fmt.Errorf("handler error: %w", err)
	}

	return nil
}

func ParseJSON(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
