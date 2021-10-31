package rmq

import (
	"errors"
	"fmt"
	"time"

	"github.com/streadway/amqp"
)

type ServerConfig struct {
	ExchangeName  string
	ExchangeType  string
	RoutingKey    string
	QueueName     string
	ConsumerName  string
	ConsumerCount int
	PrefetchCount int
	Reconnect     struct {
		MaxAttempt int
		Interval   time.Duration
	}
	DLX                  string
	QueueMode            string
	ChannelNotifyTimeout time.Duration
	ContentType          string
}

type Server struct {
	config ServerConfig
	Rabbit *Rabbit
}

// NewConsumer returns a consumer instance.
func NewServer(config ServerConfig, rabbit *Rabbit) *Server {
	return &Server{
		config: config,
		Rabbit: rabbit,
	}
}

type RabbitConfig struct {
	Schema         string
	Username       string
	Password       string
	Host           string
	Port           string
	VHost          string
	ConnectionName string
}

type Rabbit struct {
	config     RabbitConfig
	connection *amqp.Connection
}

// NewRabbit returns a RabbitMQ instance.
func NewRabbit(config RabbitConfig) *Rabbit {
	return &Rabbit{
		config: config,
	}
}

// Connect connects to RabbitMQ server.
func (r *Rabbit) Connect() error {
	if r.connection == nil || r.connection.IsClosed() {
		con, err := amqp.DialConfig(fmt.Sprintf(
			"%s://%s:%s@%s:%s/%s",
			r.config.Schema,
			r.config.Username,
			r.config.Password,
			r.config.Host,
			r.config.Port,
			r.config.VHost,
		), amqp.Config{Properties: amqp.Table{"connection_name": r.config.ConnectionName}})
		if err != nil {
			return err
		}
		r.connection = con
	}

	return nil
}

// Connection returns exiting `*amqp.Connection` instance.
func (r *Rabbit) Connection() (*amqp.Connection, error) {
	if r.connection == nil || r.connection.IsClosed() {
		return nil, errors.New("connection is not open")
	}

	return r.connection, nil
}

func (r *Rabbit) Shutdown() error {
	if r.connection != nil {
		return r.connection.Close()
	}

	return nil
}

// Channel returns a new `*amqp.Channel` instance.
func (r *Rabbit) Channel() (*amqp.Channel, error) {
	chn, err := r.connection.Channel()
	if err != nil {
		return nil, err
	}

	return chn, nil
}

func (c Server) declareCreate(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		c.config.ExchangeName,
		c.config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	args := amqp.Table{"x-queue-mode": c.config.QueueMode}
	if c.config.DLX != "" {
		args["x-dead-letter-exchange"] = c.config.DLX
	}

	if _, err := channel.QueueDeclare(
		c.config.QueueName,
		true,
		false,
		false,
		false,
		args,
	); err != nil {
		return err
	}

	if err := channel.QueueBind(
		c.config.QueueName,
		c.config.RoutingKey,
		c.config.ExchangeName,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}
