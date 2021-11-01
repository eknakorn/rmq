package rmq

import (
	"fmt"
	"log"

	"github.com/streadway/amqp"
)

// Start declares all the necessary components of the consumer and
// runs the consumers. This is called one at the application start up
// or when consumer needs to reconnects to the server.
func (c *Server) ConsumerStart() error {
	con, err := c.Rabbit.Connection()
	if err != nil {
		return err
	}
	go c.ClosedConnectionListener(con.NotifyClose(make(chan *amqp.Error)))

	channel, err := con.Channel()
	if err != nil {
		return err
	}

	if err := c.DeclareCreate(channel); err != nil {
		return err
	}

	if err := channel.Qos(c.Config.PrefetchCount, 0, false); err != nil {
		return err
	}

	for i := 1; i <= c.Config.ConsumerCount; i++ {
		id := i
		go c.consume(channel, id)
	}

	// Simulate manual connection close
	defer con.Close()

	return nil
}

// consume creates a new consumer and starts consuming the messages.
// If this is called more than once, there will be multiple consumers
// running. All consumers operate in a round robin fashion to distribute
// message load.
func (c *Server) consume(channel *amqp.Channel, id int) {
	msgs, err := channel.Consume(
		c.Config.QueueName,
		fmt.Sprintf("%s (%d/%d)", c.Config.ConsumerName, id, c.Config.ConsumerCount),
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Println(fmt.Sprintf("CRITICAL: Unable to start consumer (%d/%d)", id, c.Config.ConsumerCount))

		return
	}

	log.Println("[", id, "] Running ...")
	log.Println("[", id, "] Press CTRL+C to exit ...")

	for msg := range msgs {
		log.Println("[", id, "] Consumed:", string(msg.Body))

		if err := msg.Ack(false); err != nil {
			// TODO: Should DLX the message
			log.Println("unable to acknowledge the message, dropped", err)
		}
	}

	log.Println("[", id, "] Exiting ...")
}
