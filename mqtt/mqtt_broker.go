package mqtt

import (
	"fmt"
	"log"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type MQTTConfig struct {
	Broker   string
	ClientID string
	Username string
	Password string
}

type MQTTBroker struct {
	client paho.Client
	config MQTTConfig
}

func NewMQTTBroker(config MQTTConfig) (*MQTTBroker, error) {
	opts := paho.NewClientOptions()
	opts.AddBroker(config.Broker)
	opts.SetClientID(config.ClientID)
	opts.SetCleanSession(true)
	opts.SetAutoReconnect(true)
	opts.SetConnectionLostHandler(func(client paho.Client, err error) {
		log.Printf("MQTT connection lost: %v", err)
	})
	opts.SetOnConnectHandler(func(client paho.Client) {
		log.Println("MQTT connected")
	})

	if config.Username != "" {
		opts.SetUsername(config.Username)
	}
	if config.Password != "" {
		opts.SetPassword(config.Password)
	}

	client := paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("failed to connect to MQTT broker: %v", token.Error())
	}

	return &MQTTBroker{
		client: client,
		config: config,
	}, nil
}

func (b *MQTTBroker) Subscribe(topic string, qos byte, handler paho.MessageHandler) error {
	if token := b.client.Subscribe(topic, qos, handler); token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to subscribe to topic %s: %v", topic, token.Error())
	}
	log.Printf("Subscribed to topic: %s", topic)
	return nil
}

func (b *MQTTBroker) Publish(topic string, qos byte, retained bool, payload interface{}) error {
	token := b.client.Publish(topic, qos, retained, payload)
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("failed to publish to topic %s: %v", topic, token.Error())
	}
	return nil
}

func (b *MQTTBroker) Disconnect(quiesce uint) {
	b.client.Disconnect(quiesce)
}

func (b *MQTTBroker) IsConnected() bool {
	return b.client.IsConnected()
} 