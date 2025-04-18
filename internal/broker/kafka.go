package broker

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaBroker struct {
	reader *kafka.Reader
	writer *kafka.Writer
}

func waitForKafka(ctx context.Context, broker string, maxAttempts int) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		log.Printf("Attempting to connect to Kafka at %s...", broker)
		conn, err := kafka.Dial("tcp", broker)
		if err == nil {
			log.Printf("Connected to Kafka, checking controller...")
			// Try to get controller
			controller, err := conn.Controller()
			if err == nil {
				log.Printf("Kafka controller found at %s:%d", controller.Host, controller.Port)
				conn.Close()
				return nil
			}
			log.Printf("Failed to get controller: %v", err)
			conn.Close()
		}
		lastErr = err
		log.Printf("Attempt %d/%d: Kafka not ready yet: %v", i+1, maxAttempts, err)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("failed to connect to Kafka after %d attempts: %v", maxAttempts, lastErr)
}

func createTopic(ctx context.Context, broker string, topic string) error {
	log.Printf("Creating topic %s...", topic)
	conn, err := kafka.Dial("tcp", broker)
	if err != nil {
		return fmt.Errorf("failed to connect to Kafka: %v", err)
	}
	defer conn.Close()

	// Get controller
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %v", err)
	}

	// Connect to controller
	controllerConn, err := kafka.Dial("tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %v", err)
	}
	defer controllerConn.Close()

	// Create topic
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Ignore error if topic already exists
		if err.Error() != "Topic with this name already exists" {
			return fmt.Errorf("failed to create topic: %v", err)
		}
		log.Printf("Topic %s already exists", topic)
	} else {
		log.Printf("Topic %s created successfully", topic)
	}

	return nil
}

func NewKafkaBroker() (*KafkaBroker, error) {
	// Get Kafka broker address from environment variable
	kafkaBroker := os.Getenv("KAFKA_BROKERS")
	if kafkaBroker == "" {
		kafkaBroker = "kafka:29092" // Default to container name and port
	}
	log.Printf("Using Kafka broker at %s", kafkaBroker)

	// Wait for Kafka to be ready
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	log.Println("Waiting for Kafka to be ready...")
	if err := waitForKafka(ctx, kafkaBroker, 10); err != nil {
		return nil, fmt.Errorf("failed to wait for Kafka: %v", err)
	}
	log.Println("Kafka is ready!")

	// Create topic
	if err := createTopic(ctx, kafkaBroker, "kafka.topic"); err != nil {
		return nil, fmt.Errorf("failed to create topic: %v", err)
	}

	// Create Kafka reader
	log.Println("Creating Kafka reader...")
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBroker},
		Topic:     "kafka.topic",
		GroupID:   "message-broker-group",
		MinBytes:  10e3,
		MaxBytes:  10e6,
		MaxAttempts: 5,
		Dialer: &kafka.Dialer{
			Timeout:   30 * time.Second,
			DualStack: true,
		},
		StartOffset: kafka.FirstOffset,
		ReadBackoffMin: 100 * time.Millisecond,
		ReadBackoffMax: 1 * time.Second,
		MaxWait: 10 * time.Second,
	})

	// Create Kafka writer
	log.Println("Creating Kafka writer...")
	writer := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    "kafka.topic",
		Balancer: &kafka.LeastBytes{},
		MaxAttempts: 5,
		BatchSize:   1,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		Async:        false,
		Compression:  kafka.Snappy,
		Transport: &kafka.Transport{
			DialTimeout: 30 * time.Second,
		},
	}

	return &KafkaBroker{
		reader: reader,
		writer: writer,
	}, nil
}

func (k *KafkaBroker) Consume(ctx context.Context, handler func(message []byte) error) error {
	log.Println("Starting Kafka consumer...")
	for {
		msg, err := k.reader.ReadMessage(ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Println("Kafka read timeout, retrying...")
				time.Sleep(time.Second)
				continue
			}
			log.Printf("Consumer error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		log.Printf("Received message: %s", string(msg.Value))
		if err := handler(msg.Value); err != nil {
			log.Printf("Error handling message: %v", err)
		}
	}
}

func (k *KafkaBroker) Publish(ctx context.Context, message []byte) error {
	log.Printf("Publishing message: %s", string(message))
	return k.writer.WriteMessages(ctx, kafka.Message{
		Value: message,
	})
}

func (k *KafkaBroker) Close() error {
	log.Println("Closing Kafka connections...")
	if err := k.reader.Close(); err != nil {
		return fmt.Errorf("error closing reader: %v", err)
	}
	if err := k.writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}
	return nil
} 