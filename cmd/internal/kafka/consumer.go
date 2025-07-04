package kafka

import (
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/sirupsen/logrus"
)

const (
	consumerGroup = "my-group"
	sessionTimeout = 7000 // ms
	noTimeout = -1 
)

type Handler interface {
	HandleMessage(message []byte, offset kafka.Offset) error
}

type Consumer struct {
	consumer *kafka.Consumer
	handler  Handler
	stop bool
}

func NewConsumer(handler Handler, address []string, topic, consumerGroup string) (*Consumer, error) {
cfg := &kafka.ConfigMap{
	"bootstrap.servers": strings.Join(address, ","),
	"group.id":          consumerGroup,
	"session.timeout.ms": sessionTimeout,
	"enable.auto.offset.store": false,
	"enable.auto.commit": true,
	"auto.commit.interval.ms": 5000, // ms
	"auto.offset.reset": "earliest", // Start reading from the earliest message
}
c, err := kafka.NewConsumer(cfg)

	if err != nil {
		return nil, err
	}

	if err = c.Subscribe(topic, nil); err != nil {
		return nil, err
	}

	return &Consumer{
		consumer: c,
		handler:  handler,
	}, nil
}

func (c *Consumer) Start() {
	for {
		if c.stop {
			break
		}
		kafkaMsg, err := c.consumer.ReadMessage(noTimeout)
		if err != nil {
			logrus.Error(err)
		}
		if kafkaMsg == nil {
			continue
		}
		if err = c.handler.HandleMessage(kafkaMsg.Value, kafkaMsg.TopicPartition.Offset); err != nil {
			logrus.Error(err)
			continue
		}

		if _, err = c.consumer.StoreMessage(kafkaMsg); err != nil {
			logrus.Errorf("failed to store message: %v", err)
			continue
		}
	}
}

func (c *Consumer) Stop() error {
	c.stop = true
	if _, err := c.consumer.Commit(); err != nil {
		return err
	}
	logrus.Info("Committing offset")
	return c.consumer.Close()
}