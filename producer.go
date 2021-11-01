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

	if err := a.DeclareCreate(channel); err != nil {
		return err
	}

	return nil
}

func (s Server) Publish(message []byte) error {

	con, err := s.Rabbit.Connection()
	if err != nil {
		return errors.Wrap(err, "failed to connect rabbitMQ")
	}

	channel, err := con.Channel()
	if err != nil {
		return errors.Wrap(err, "failed to open channel rabbitMQ")
	}

	if err := channel.Confirm(false); err != nil {
		return errors.Wrap(err, "failed to put channel in confirmation mode")
	}

	if err := channel.Publish(
		s.Config.ExchangeName,
		s.Config.ExchangeType,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			MessageId:    uuid.New().String(),
			ContentType:  "application/json",
			Body:         message,
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
	case <-time.After(s.Config.ChannelNotifyTimeout):
		log.Println("message delivery confirmation to exchange/queue timed out")
	}

	return nil
}
