package kafka

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaConfig struct {
	BootstrapServers string
	GroupID         string
	AutoOffsetReset string
}

type KafkaClient struct {
	writer *kafka.Writer
	config KafkaConfig
}

func NewKafkaClient(config KafkaConfig) (*KafkaClient, error) {
	// Create writer
	w := &kafka.Writer{
		Addr:     kafka.TCP(config.BootstrapServers),
		Balancer: &kafka.LeastBytes{},
	}

	return &KafkaClient{
		writer: w,
		config: config,
	}, nil
}

func (k *KafkaClient) Produce(topic string, message []byte) error {
	err := k.writer.WriteMessages(context.Background(),
		kafka.Message{
			Topic: topic,
			Value: message,
		},
	)
	if err != nil {
		return fmt.Errorf("failed to produce message: %v", err)
	}
	return nil
}

func (k *KafkaClient) Consume(topic string, handler func([]byte) error) error {
	// Create a new reader for this topic
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{k.config.BootstrapServers},
		GroupID:   k.config.GroupID,
		Topic:     topic,
		MinBytes:  10e3,
		MaxBytes:  10e6,
		MaxWait:   time.Second,
	})

	go func() {
		defer reader.Close()
		for {
			msg, err := reader.ReadMessage(context.Background())
			if err != nil {
				log.Printf("Consumer error: %v", err)
				continue
			}

			if err := handler(msg.Value); err != nil {
				log.Printf("Error handling message: %v", err)
			}
		}
	}()

	return nil
}

func (k *KafkaClient) Close() {
	if k.writer != nil {
		k.writer.Close()
	}
} 