package rmq

import (
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

func (a Server) Setup() error {
	channel, err := a.Rabbit.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel")
	}
	defer channel.Close()

	if err := a.declareCreate(channel); err != nil {
		return err
	}

	return nil
}

func (c Server) Publish(channel *amqp.Channel, message string) error {

	if err := channel.Confirm(false); err != nil {
		return errors.Wrap(err, "failed to put channel in confirmation mode")
	}

	if err := channel.Publish(
		c.config.ExchangeName,
		c.config.ExchangeType,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    uuid.New().String(),
			ContentType:  c.config.ExchangeType,
			Body:         []byte(message),
		},
	); err != nil {
		return errors.Wrap(err, "failed to publish message")
	}

	select {
	case ntf := <-channel.NotifyPublish(make(chan amqp.Confirmation, 1)):
		if !ntf.Ack {
			return errors.New("failed to deliver message to exchange/queue")
		}
	case <-channel.NotifyReturn(make(chan amqp.Return)):
		return errors.New("failed to deliver message to exchange/queue")
	case <-time.After(c.config.ChannelNotifyTimeout):
		log.Println("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}
