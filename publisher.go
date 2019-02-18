package rabbitmq

import (
	"errors"

	"github.com/streadway/amqp"
)

var (
	ErrorFailedPublish    = errors.New("failed message delivery")
	ErrorInconsistentSize = errors.New("inconsistent messages or routing keys count")
)

type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	config  *Config
}

func NewPublisher(conf *Config) (*Publisher, error) {
	p := &Publisher{config: conf}

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

func (p *Publisher) Publish(msg []byte, routingKeys []string) error {
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

func (p *Publisher) MultiPublish(msgs [][]byte, routingKeysList [][]string) error {
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

func (p *Publisher) Close() error {
	if p.conn == nil {
		return nil
	}

	return p.conn.Close()
}
