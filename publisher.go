package rabbitmq

import (
	"errors"
	"github.com/streadway/amqp"
)

var (
	ErrorFailedPublish = errors.New("failed message delivery")
)

type Publisher struct {
	conn   *Connection
	config *Config
}

func NewPublisher(conf *Config) (*Publisher, error) {
	p := &Publisher{config: conf}

	var err error
	p.conn, err = NewConnection(conf.URLs)
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Publish(msg []byte, routingKey string) error {
	ch, err := p.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	err = ch.ExchangeDeclare(
		p.config.Exchange,
		p.config.ExchangeType,
		true,  // durable
		false, //auto delete
		false, // internal
		false, // noWait
		nil)
	if err != nil {
		return err
	}

	err = ch.Confirm(false)
	if err != nil {
		return nil
	}

	confirm := make(chan amqp.Confirmation, 1)
	if p.config.PublisherConfirm {
		ch.NotifyPublish(confirm)
	}

	err = ch.Publish(
		p.config.Exchange,
		routingKey,
		false, // mandatory
		false, //immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			Body:         msg,
		})
	if err != nil {
		return err
	}

	if p.config.PublisherConfirm {
		confirmed := <-confirm
		if !confirmed.Ack {
			return ErrorFailedPublish
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
