package main

import (
	"context"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

var (
	topic = os.Getenv("SUBSCRIPTION_TOPIC")
	host  = os.Getenv("SUBSCRIPTION_HOST")
)

func main() {
	conn := newConnection(topic)
	defer conn.reader.Close()

	consume(conn)
}

func consume(conn *kafkaConnection) {
	m, err := conn.reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	println(string(m.Value))
}

type kafkaConnection struct {
	reader *kafka.Reader
}

func newConnection(topic string) *kafkaConnection {
	groupID := topic
	return &kafkaConnection{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:     []string{host},
			GroupID:     groupID,
			Topic:       topic,
			MaxAttempts: 3,
		}),
	}
}
