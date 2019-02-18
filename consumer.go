package rabbitmq

import (
	"context"
	"log"

	"github.com/streadway/amqp"
)

type Consumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
	queue   string
}

func NewConsumer(conf *Config) (*Consumer, error) {
	c := &Consumer{config: conf}

	var err error
	c.conn, err = NewConnection(conf.URLs)
	if err != nil {
		return nil, err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return nil, err
	}

	if err := c.channel.ExchangeDeclare(
		c.config.Exchange,
		c.config.ExchangeType,
		true,  // durable
		false, // auto delete
		false, // internal
		false, // no wait
		nil); err != nil {
		return nil, err
	}

	var queue amqp.Queue

	if c.config.DeadLetterExchange != "" {
		args := amqp.Table{
			"x-dead-letter-exchange": c.config.DeadLetterExchange,
		}

		queue, err = c.channel.QueueDeclare(
			c.config.QueueName,
			true,  // durable
			false, // auto delete
			false, // exclusive
			false, // no wait
			args)
	} else {
		queue, err = c.channel.QueueDeclare(
			c.config.QueueName,
			true,
			false,
			false,
			false,
			nil)
	}

	if err != nil {
		return nil, err
	}

	c.queue = queue.Name

	for _, bindingKey := range c.config.BindingKeys {
		err = c.channel.QueueBind(
			c.config.QueueName,
			bindingKey,
			c.config.Exchange,
			false, // no wait
			nil)
		if err != nil {
			return nil, err
		}
	}

	return c, nil
}

type processFn func(payload []byte) error

func (c *Consumer) Consume(ctx context.Context, fn processFn) (int64, error) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var msgCount int64

	deliveries, err := c.channel.Consume(
		c.queue,
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
		case msg, ok := <-deliveries:
			if !ok {
				return msgCount, nil
			}
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
}

func (c *Consumer) Close() error {
	if c.conn == nil {
		return nil
	}

	return c.conn.Close()
}
