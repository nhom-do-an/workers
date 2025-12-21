package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"workers/config"
	"workers/internal/clickhouse"
	"workers/internal/postgres"
	"workers/internal/rabbitmq"
	"workers/internal/workers"
	"workers/pkg/logger"
)

func main() {
	// Initialize logger
	logger.Init()
	log.Println("🚀 Starting Workers Service...")

	// Load configuration
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	if err := cfg.Validate(); err != nil {
		log.Fatalf("Invalid configuration: %v", err)
	}

	log.Printf("✓ Configuration loaded")
	log.Printf("  - RabbitMQ: %s", cfg.RabbitMQ.URL)
	log.Printf("  - ClickHouse: %s:%d/%s", cfg.ClickHouse.Host, cfg.ClickHouse.Port, cfg.ClickHouse.Database)
	log.Printf("  - Postgres: %s:%d/%s", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)

	// Connect to Postgres
	pgClient, err := postgres.NewClient(postgres.PostgresConfig{
		Host:     cfg.Postgres.Host,
		Port:     cfg.Postgres.Port,
		Database: cfg.Postgres.Database,
		Username: cfg.Postgres.Username,
		Password: cfg.Postgres.Password,
	})
	if err != nil {
		log.Fatalf("Failed to connect to Postgres: %v", err)
	}
	defer pgClient.Close()
	log.Println("✓ Connected to Postgres")

	// Connect to ClickHouse
	chClient, err := clickhouse.NewClient(cfg.ClickHouse)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer chClient.Close()
	log.Println("✓ Connected to ClickHouse")

	// Create RabbitMQ consumers
	orderConsumer, err := rabbitmq.NewConsumer(cfg.RabbitMQ)
	if err != nil {
		log.Fatalf("Failed to create order consumer: %v", err)
	}
	defer orderConsumer.Close()

	lineItemConsumer, err := rabbitmq.NewConsumer(cfg.RabbitMQ)
	if err != nil {
		log.Fatalf("Failed to create line item consumer: %v", err)
	}
	defer lineItemConsumer.Close()
	log.Println("✓ Connected to RabbitMQ")

	// Create workers
	orderWorker := workers.NewOrderWorker(orderConsumer, chClient, pgClient, cfg.RabbitMQ.OrderQueue)
	lineItemWorker := workers.NewLineItemWorker(lineItemConsumer, chClient, pgClient, cfg.RabbitMQ.LineItemQueue)

	// Start workers in goroutines
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := orderWorker.Start(); err != nil {
			log.Printf("Order worker error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := lineItemWorker.Start(); err != nil {
			log.Printf("LineItem worker error: %v", err)
		}
	}()

	log.Println("✓ All workers started successfully")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Println("🛑 Shutting down workers...")
	wg.Wait()
	log.Println("✓ Workers stopped gracefully")
}
