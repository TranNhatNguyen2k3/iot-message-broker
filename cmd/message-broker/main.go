package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"message-broker/internal/broker"
)

func main() {
	// Create context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create Kafka broker
	kafkaBroker, err := broker.NewKafkaBroker()
	if err != nil {
		log.Fatalf("Failed to create Kafka broker: %v", err)
	}
	defer kafkaBroker.Close()

	// Create MQTT broker
	mqttBroker, err := broker.NewMQTTBroker()
	if err != nil {
		log.Fatalf("Failed to create MQTT broker: %v", err)
	}
	defer mqttBroker.Close()

	// Start consuming from Kafka
	go func() {
		err := kafkaBroker.Consume(ctx, func(message []byte) error {
			log.Printf("Received from Kafka: %s", string(message))
			return mqttBroker.Publish(ctx, message)
		})
		if err != nil {
			log.Printf("Kafka consumer error: %v", err)
		}
	}()

	// Start consuming from MQTT
	go func() {
		err := mqttBroker.Consume(ctx, func(message []byte) error {
			log.Printf("Received from MQTT: %s", string(message))
			return kafkaBroker.Publish(ctx, message)
		})
		if err != nil {
			log.Printf("MQTT consumer error: %v", err)
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
	cancel()
	time.Sleep(2 * time.Second) // Give time for cleanup
} 