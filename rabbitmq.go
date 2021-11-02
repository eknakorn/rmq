package rmq

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/streadway/amqp"
)

type Config struct {
	ExchangeName         string
	DeadLetterExchange   string
	ExchangeType         string
	RoutingKey           string
	DeadLetterRoutingKey string
	QueueName            string
	DeadLetterQueueName  string
	ConsumerName         string
	ConsumerCount        int
	PrefetchCount        int
	QueueMode            string
	ChannelNotifyTimeout time.Duration
	ContentType          string
	Reconnect            struct {
		MaxAttempt int
		Interval   time.Duration
	}
}

type Server struct {
	Config Config
	Rabbit *Rabbit
}

// NewConsumer returns a consumer instance.
func NewServer(Config Config, rabbit *Rabbit) *Server {
	return &Server{
		Config: Config,
		Rabbit: rabbit,
	}
}

type ServerConfig struct {
	Schema   string
	Username string
	Password string
	Host     string
	Port     string
}

type Rabbit struct {
	Config     ServerConfig
	connection *amqp.Connection
}

// NewRabbit returns a RabbitMQ instance.
func NewRabbit(Config ServerConfig) *Rabbit {
	return &Rabbit{
		Config: Config,
	}
}

// Connect connects to RabbitMQ server.
func (r *Rabbit) Connect() error {
	if r.connection == nil || r.connection.IsClosed() {

		url := fmt.Sprintf(
			"%s://%s:%s@%s:%s",
			r.Config.Schema,
			r.Config.Username,
			r.Config.Password,
			r.Config.Host,
			r.Config.Port,
		)

		log.Printf("INFO: amqp url: %s", url)

		con, err := amqp.Dial(url)
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

// closedConnectionListener attempts to reconnect to the server and
// reopens the channel for set amount of time if the connection is
// closed unexpectedly. The attempts are spaced at equal intervals.
func (c *Server) ClosedConnectionListener(closed <-chan *amqp.Error) {
	log.Println("INFO: Watching closed connection")

	// If you do not want to reconnect in the case of manual disconnection
	// via RabbitMQ UI or Server restart, handle `amqp.ConnectionForced`
	// error code.
	err := <-closed
	if err != nil {
		log.Println("INFO: Closed connection:", err.Error())

		var i int

		for i = 0; i < c.Config.Reconnect.MaxAttempt; i++ {
			log.Println("INFO: Attempting to reconnect")

			if err := c.Rabbit.Connect(); err == nil {
				log.Println("INFO: Reconnected")

				if err := c.ConsumerStart(); err == nil {
					break
				}
			}

			time.Sleep(c.Config.Reconnect.Interval)
		}

		if i == c.Config.Reconnect.MaxAttempt {
			log.Println("CRITICAL: Giving up reconnecting")

			return
		}
	} else {
		log.Println("INFO: Connection closed normally, will not reconnect")
		os.Exit(0)
	}
}

func (c Server) DeclareCreate(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		c.Config.QueueName,
		c.Config.ExchangeType,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	args := amqp.Table{"x-queue-mode": c.Config.QueueMode}
	if c.Config.DeadLetterExchange != "" {
		args["x-dead-letter-exchange"] = c.Config.DeadLetterRoutingKey
	}
	if c.Config.DeadLetterRoutingKey != "" {
		args["x-dead-letter-routing-key"] = c.Config.DeadLetterRoutingKey
	}

	if _, err := channel.QueueDeclare(
		c.Config.DeadLetterQueueName,
		true,
		false,
		false,
		false,
		args,
	); err != nil {
		return err
	}

	if err := channel.QueueBind(
		c.Config.DeadLetterQueueName,
		c.Config.DeadLetterRoutingKey,
		c.Config.DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return err
	}

	return nil
}
