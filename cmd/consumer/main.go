package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/HollyEllmo/go_kafka/cmd/internal/handler"
	"github.com/HollyEllmo/go_kafka/cmd/internal/kafka"
	"github.com/sirupsen/logrus"
)

const (
	topic         = "my-topic"
	consumerGroup = "my-consumer-group"
)

var address = []string{
	"localhost:9091",
	"localhost:9092",
	"localhost:9093",
}

func main() {
	h := handler.NewHandler()
	c, err := kafka.NewConsumer(h, address, topic, consumerGroup)
	if err != nil {
		logrus.Fatal(err)
	}

	go func() {
		c.Start()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logrus.Fatal(c.Stop())

}