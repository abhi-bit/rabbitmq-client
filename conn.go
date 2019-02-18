package rabbitmq

import (
	"math/rand"

	"github.com/streadway/amqp"
)

func NewConnection(urls []string) (*amqp.Connection, error) {
	var index int
	if len(urls) > 1 {
		index = rand.Intn(len(urls) - 1)
	} else {
		index = 0
	}

	url := urls[index]
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
