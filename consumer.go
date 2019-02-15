package rabbitmq

import (
	"context"
	"github.com/streadway/amqp"
	"log"
)

type Consumer struct {
	conn   *Connection
	config *Config
}

func NewConsumer(conf *Config) (*Consumer, error) {
	c := &Consumer{config: conf}

	var err error
	c.conn, err = NewConnection(conf.URLs)
	if err != nil {
		return nil, err
	}
	return c, nil
}

type processFn func(payload []byte) error

func (c *Consumer) Consume(ctx context.Context, fn processFn) (int64, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var msgCount int64
	ch, err := c.conn.Channel()
	if err != nil {
		return 0, err
	}

	if err := ch.ExchangeDeclare(
		c.config.Exchange,
		c.config.ExchangeType,
		true,  // durable
		false, // auto delete
		false, // internal
		false, // no wait
		nil); err != nil {
		return 0, err
	}
	log.Println()

	var queue amqp.Queue

	if c.config.DeadLetterExchange != "" {
		args := amqp.Table{
			"x-dead-letter-exchange": c.config.DeadLetterExchange,
		}
		log.Println()

		queue, err = ch.QueueDeclare(
			c.config.QueueName,
			true,  // durable
			false, // auto delete
			false, // exclusive
			false, // no wait
			args)
	} else {
		queue, err = ch.QueueDeclare(
			c.config.QueueName,
			true,
			false,
			false,
			false,
			nil)
	}

	if err != nil {
		return 0, err
	}
	log.Println()

	for _, bindingKey := range c.config.BindingKeys {
		err = ch.QueueBind(
			c.config.QueueName,
			bindingKey,
			c.config.Exchange,
			false, // no wait
			nil)
		if err != nil {
			return 0, err
		}
		log.Println()

	}

	deliveries, err := ch.Consume(
		queue.Name,
		"",    // consumer tag
		false, // auto ack
		false, // exclusive
		false, // no local
		false, // no wait
		nil)
	if err != nil {
		return 0, err
	}

	for {
		select {
		case <-ctx.Done():
			return msgCount, nil
		case msg := <-deliveries:
			err = fn(msg.Body)
			if err != nil {
				rErr := msg.Reject(false)
				if rErr != nil {
					return msgCount, rErr
				}
			} else {
				aErr := msg.Ack(false)
				if aErr != nil {
					return msgCount, aErr
				}
			}
			msgCount++
		}
	}

	/*for msg := range deliveries {
		err = fn(msg.Body)
		if err != nil {
			rErr := msg.Reject(false)
			if rErr != nil {
				return msgCount, rErr
			}
		} else {
			aErr := msg.Ack(false)
			if aErr != nil {
				return msgCount, aErr
			}
		}
		log.Println()

		msgCount++
	}*/

	log.Println()
	return msgCount, nil
}

func (c *Consumer) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}
