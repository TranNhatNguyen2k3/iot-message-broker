package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaBroker = "localhost:9092"
	mqttBroker  = "tcp://localhost:1883"
)

func waitForKafka(ctx context.Context, maxAttempts int) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		conn, err := kafka.Dial("tcp", kafkaBroker)
		if err == nil {
			// Try to get controller
			_, err := conn.Controller()
			if err == nil {
				conn.Close()
				return nil
			}
			conn.Close()
		}
		lastErr = err
		log.Printf("Attempt %d/%d: Kafka not ready yet: %v", i+1, maxAttempts, err)
		time.Sleep(5 * time.Second)
	}
	return fmt.Errorf("failed to connect to Kafka after %d attempts: %v", maxAttempts, lastErr)
}

func createKafkaTopic(ctx context.Context, topic string) error {
	// Connect to Kafka
	conn, err := kafka.Dial("tcp", kafkaBroker)
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
	}

	return nil
}

func main() {
	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Wait for Kafka to be ready
	log.Println("Waiting for Kafka to be ready...")
	if err := waitForKafka(ctx, 10); err != nil {
		log.Fatal(err)
	}
	log.Println("Kafka is ready!")

	// MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker(mqttBroker).
		SetClientID("test-client").
		SetCleanSession(true).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(client mqtt.Client, err error) {
			log.Printf("MQTT connection lost: %v", err)
		}).
		SetOnConnectHandler(func(client mqtt.Client) {
			log.Println("MQTT connected")
		})

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	defer mqttClient.Disconnect(250)

	// Kafka topic
	topic := "kafka.topic"

	// Create Kafka topic
	log.Println("Creating Kafka topic...")
	if err := createKafkaTopic(ctx, topic); err != nil {
		log.Fatal(err)
	}
	log.Println("Kafka topic created!")

	// Kafka writer
	kafkaWriter := &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
		// Add retry settings
		MaxAttempts: 3,
		BatchSize:   1,
		BatchTimeout: 10 * time.Millisecond,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		Async:        false, // Make writes synchronous
	}
	defer kafkaWriter.Close()

	// Kafka reader
	kafkaReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{kafkaBroker},
		Topic:     topic,
		GroupID:   "test-group",
		MinBytes:  10e3,
		MaxBytes:  10e6,
		// Add retry settings
		MaxAttempts: 3,
		Dialer: &kafka.Dialer{
			Timeout:   10 * time.Second,
			DualStack: true,
		},
		StartOffset: kafka.FirstOffset, // Start from the beginning
	})
	defer kafkaReader.Close()

	// Subscribe to MQTT topic
	if token := mqttClient.Subscribe("mqtt/topic", 0, func(client mqtt.Client, msg mqtt.Message) {
		fmt.Printf("\nReceived MQTT message: %s\n", msg.Payload())
		fmt.Print("Enter command (mqtt/kafka/exit): ")
	}); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Start a goroutine to read Kafka messages
	go func() {
		for {
			msg, err := kafkaReader.ReadMessage(ctx)
			if err != nil {
				if err == context.DeadlineExceeded {
					log.Println("Kafka read timeout, retrying...")
					continue
				}
				log.Printf("Error reading from Kafka: %v", err)
				time.Sleep(time.Second) // Wait before retrying
				continue
			}
			fmt.Printf("\nReceived Kafka message: %s\n", string(msg.Value))
			fmt.Print("Enter command (mqtt/kafka/exit): ")
		}
	}()

	fmt.Println("Message Broker Test Client")
	fmt.Println("Commands:")
	fmt.Println("  mqtt  - Send message to MQTT")
	fmt.Println("  kafka - Send message to Kafka")
	fmt.Println("  exit  - Exit program")
	fmt.Print("Enter command (mqtt/kafka/exit): ")

	reader := bufio.NewReader(os.Stdin)
	for {
		command, _ := reader.ReadString('\n')
		command = strings.TrimSpace(command)

		switch command {
		case "mqtt":
			fmt.Print("Enter MQTT message: ")
			message, _ := reader.ReadString('\n')
			message = strings.TrimSpace(message)
			token := mqttClient.Publish("mqtt/topic", 0, false, message)
			token.Wait()
			fmt.Println("Message sent to MQTT")

		case "kafka":
			fmt.Print("Enter Kafka message: ")
			message, _ := reader.ReadString('\n')
			message = strings.TrimSpace(message)
			err := kafkaWriter.WriteMessages(ctx, kafka.Message{
				Value: []byte(message),
			})
			if err != nil {
				fmt.Printf("Error sending to Kafka: %v\n", err)
			} else {
				fmt.Println("Message sent to Kafka")
			}

		case "exit":
			fmt.Println("Exiting...")
			return

		default:
			fmt.Println("Unknown command. Use mqtt, kafka, or exit")
		}

		fmt.Print("Enter command (mqtt/kafka/exit): ")
	}
} 