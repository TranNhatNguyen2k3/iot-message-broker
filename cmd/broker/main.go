package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	paho "github.com/eclipse/paho.mqtt.golang"
	"message-broker/kafka"
	"message-broker/mqtt"
)

func main() {
	// Get broker addresses from environment variables or use defaults
	mqttBrokerAddr := getEnv("MQTT_BROKER", "tcp://localhost:1883")
	kafkaBrokerAddr := getEnv("KAFKA_BROKER", "localhost:9092")

	// MQTT Configuration
	mqttConfig := mqtt.MQTTConfig{
		Broker:   mqttBrokerAddr,
		ClientID: "mqtt-kafka-bridge",
	}

	// Kafka Configuration
	kafkaConfig := kafka.KafkaConfig{
		BootstrapServers: kafkaBrokerAddr,
		GroupID:         "mqtt-kafka-bridge-group",
		AutoOffsetReset: "earliest",
	}

	// Create MQTT broker
	mqttBroker, err := mqtt.NewMQTTBroker(mqttConfig)
	if err != nil {
		log.Fatalf("Failed to create MQTT broker: %v", err)
	}
	defer mqttBroker.Disconnect(250)

	// Create Kafka client
	kafkaClient, err := kafka.NewKafkaClient(kafkaConfig)
	if err != nil {
		log.Fatalf("Failed to create Kafka client: %v", err)
	}
	defer kafkaClient.Close()

	// MQTT to Kafka bridge
	mqttTopic := "mqtt/topic"
	kafkaTopic := "kafka.topic"

	// Subscribe to MQTT topic and forward messages to Kafka
	if err := mqttBroker.Subscribe(mqttTopic, 0, func(client paho.Client, msg paho.Message) {
		log.Printf("Received MQTT message on topic %s: %s", msg.Topic(), string(msg.Payload()))
		
		if err := kafkaClient.Produce(kafkaTopic, msg.Payload()); err != nil {
			log.Printf("Failed to forward message to Kafka: %v", err)
		}
	}); err != nil {
		log.Fatalf("Failed to subscribe to MQTT topic: %v", err)
	}

	// Consume from Kafka and publish to MQTT
	if err := kafkaClient.Consume(kafkaTopic, func(message []byte) error {
		log.Printf("Received Kafka message: %s", string(message))
		
		if err := mqttBroker.Publish(mqttTopic, 0, false, message); err != nil {
			log.Printf("Failed to forward message to MQTT: %v", err)
		}
		return nil
	}); err != nil {
		log.Fatalf("Failed to consume from Kafka topic: %v", err)
	}

	log.Printf("Bridge is running. MQTT topic: %s, Kafka topic: %s", mqttTopic, kafkaTopic)
	log.Printf("MQTT Broker: %s, Kafka Broker: %s", mqttBrokerAddr, kafkaBrokerAddr)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	log.Println("Shutting down...")
}

func getEnv(key, defaultValue string) string {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}
	return value
} 