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
	c1, err := kafka.NewConsumer(h, address, topic, consumerGroup, 1)
	if err != nil {
		logrus.Fatal(err)
	}

	c2, err := kafka.NewConsumer(h, address, topic, consumerGroup, 2)
	if err != nil {
		logrus.Fatal(err)
	}

	c3, err := kafka.NewConsumer(h, address, topic, consumerGroup, 3)
	if err != nil {
		logrus.Fatal(err)
	}

	go func() {
		c1.Start()
	}()

	go func() {
		c2.Start()
	}()

	go func() {
		c3.Start()
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logrus.Info("Shutting down consumers...")
	
	if err := c1.Stop(); err != nil {
		logrus.Errorf("Error stopping consumer 1: %v", err)
	}
	if err := c2.Stop(); err != nil {
		logrus.Errorf("Error stopping consumer 2: %v", err)
	}
	if err := c3.Stop(); err != nil {
		logrus.Errorf("Error stopping consumer 3: %v", err)
	}
	
	logrus.Info("All consumers stopped successfully")

}