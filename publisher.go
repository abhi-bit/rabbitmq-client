package rabbitmq

import (
	"errors"
	"log"

	"github.com/streadway/amqp"
)

var (
	ErrorFailedPublish    = errors.New("failed message delivery")
	ErrorInconsistentSize = errors.New("inconsistent messages or routing keys count")
)

type RMQPublisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
}

type Publisher interface {
	Publish(msg []byte, routingKeys []string) error
	MultiPublish(msgs [][]byte, routingKeysList [][]string) error
	Close() error
}

func NewPublishers(conf *Config, count int) ([]*RMQPublisher, error) {
	var publishers []*RMQPublisher

	for i := 0; i < count; i++ {
		p, err := NewPublisher(conf)
		if err != nil {
			for _, p := range publishers {
				cErr := p.Close()
				if cErr != nil {
					log.Fatalf("Failed to close publisher connection, err: %v", err)
				}
			}
			return nil, err
		}
		publishers = append(publishers, p)
	}
	return publishers, nil
}

func NewPublisher(conf *Config) (*RMQPublisher, error) {
	p := &RMQPublisher{config: conf}

	var err error
	p.conn, err = NewConnection(conf.URLs)
	if err != nil {
		return nil, err
	}

	p.channel, err = p.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = p.channel.ExchangeDeclare(
		p.config.Exchange,
		p.config.ExchangeType,
		true,  // durable
		false, //auto delete
		false, // internal
		false, // noWait
		nil)
	if err != nil {
		return nil, err
	}

	err = p.channel.Confirm(false)
	if err != nil {
		return nil, err
	}

	return p, nil
}

func (p *RMQPublisher) Publish(msg []byte, routingKeys []string) error {
	confirm := make(chan amqp.Confirmation, 1)
	if p.config.PublisherConfirm {
		p.channel.NotifyPublish(confirm)
	}

	for _, routingKey := range routingKeys {
		if err := p.channel.Publish(
			p.config.Exchange,
			routingKey,
			false, // mandatory
			false, //immediate
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				Body:         msg,
			}); err != nil {
			return err
		}

		if p.config.PublisherConfirm {
			confirmed := <-confirm
			if !confirmed.Ack {
				return ErrorFailedPublish
			}
		}
	}
	return nil
}

func (p *RMQPublisher) MultiPublish(msgs [][]byte, routingKeysList [][]string) error {
	if len(msgs) != len(routingKeysList) {
		return ErrorInconsistentSize
	}

	for index, msg := range msgs {
		err := p.Publish(msg, routingKeysList[index])
		if err != nil {
			return err
		}
	}

	return nil
}

func (p *RMQPublisher) Close() error {
	if p.conn == nil {
		return nil
	}

	return p.conn.Close()
}
