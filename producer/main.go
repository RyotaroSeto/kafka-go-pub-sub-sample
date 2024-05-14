package main

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
)

var (
	topic = os.Getenv("SUBSCRIPTION_TOPIC")
	host  = os.Getenv("SUBSCRIPTION_HOST")
)

func main() {
	conn := newConnection()
	defer conn.writer.Close()

	produce(conn)
}

func produce(conn *kafkaConnection) {
	conn.writer.WriteMessages(context.Background(), kafka.Message{
		Topic: topic,
		Key:   []byte("Key"),
		Value: []byte("Hello World!"),
	})
}

type kafkaConnection struct {
	writer *kafka.Writer
}

func newConnection() *kafkaConnection {
	return &kafkaConnection{
		writer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:     []string{host},
			MaxAttempts: 3,
		}),
	}
}
