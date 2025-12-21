package config

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Config struct {
	RabbitMQ   RabbitMQConfig
	ClickHouse ClickHouseConfig
	Postgres   PostgresConfig
}

type RabbitMQConfig struct {
	URL           string
	OrderQueue    string
	LineItemQueue string
	PrefetchCount int
}

type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

type PostgresConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

func LoadConfig() (*Config, error) {
	// Load .env file if exists
	_ = godotenv.Load()

	prefetchCount, _ := strconv.Atoi(getEnv("RABBITMQ_PREFETCH_COUNT", "10"))
	chPort, _ := strconv.Atoi(getEnv("CLICKHOUSE_PORT", "9000"))
	pgPort, _ := strconv.Atoi(getEnv("POSTGRES_PORT", "5432"))

	return &Config{
		RabbitMQ: RabbitMQConfig{
			URL:           getEnv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/"),
			OrderQueue:    getEnv("RABBITMQ_ORDER_QUEUE", "dwh.orders.v2"),
			LineItemQueue: getEnv("RABBITMQ_LINE_ITEM_QUEUE", "dwh.line_item"),
			PrefetchCount: prefetchCount,
		},
		ClickHouse: ClickHouseConfig{
			Host:     getEnv("CLICKHOUSE_HOST", "clickhouse"),
			Port:     chPort,
			Database: getEnv("CLICKHOUSE_DATABASE", "ocm_dev"),
			Username: getEnv("CLICKHOUSE_USERNAME", "default"),
			Password: getEnv("CLICKHOUSE_PASSWORD", ""),
		},
		Postgres: PostgresConfig{
			Host:     getEnv("POSTGRES_HOST", "postgres"),
			Port:     pgPort,
			Database: getEnv("POSTGRES_DATABASE", "ocm_dev"),
			Username: getEnv("POSTGRES_USERNAME", "postgres"),
			Password: getEnv("POSTGRES_PASSWORD", "postgres"),
		},
	}, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func (c *Config) Validate() error {
	if c.RabbitMQ.URL == "" {
		log.Fatal("RABBITMQ_URL is required")
	}
	if c.ClickHouse.Host == "" {
		log.Fatal("CLICKHOUSE_HOST is required")
	}
	if c.Postgres.Host == "" {
		log.Fatal("POSTGRES_HOST is required")
	}
	return nil
}
