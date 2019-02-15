package rabbitmq

import (
	"math/rand"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"source.golabs.io/core/alfred/logger"
)

const healthCheckInterval = 5 * time.Second

type Connection struct {
	conn *amqp.Connection
	quit chan struct{}
	url  string
	sync.RWMutex
}

func (c *Connection) Channel() (*amqp.Channel, error) {
	c.RLock()
	defer c.RUnlock()

	return c.conn.Channel()
}

func (c *Connection) handleConnectionFailures(interval time.Duration) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[Connection.handleConnectionFailures] recovered from connection retry panic")
			c.checkConnection(interval)
		}
	}()

	c.checkConnection(interval)
}

func (c *Connection) checkConnection(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ch, err := c.Channel()
			if err == nil {
				_ = ch.Close()
				continue
			}

			if err := c.reconnect(); err != nil {
				logger.Errorf("[Connection.checkConnection] failed to establish connection to rabbitmq. retrying in %v seconds", interval.Seconds())
			}

		case <-c.quit:
			return
		}
	}
}

func (c *Connection) reconnect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}

	c.Lock()
	defer c.Unlock()
	c.conn = conn

	logger.Infof("[Connection.reconnect] successfully reconnected to rabbitmq")
	return nil
}

func (c *Connection) Close() error {
	c.RLock()
	defer c.RUnlock()

	close(c.quit)
	return c.conn.Close()
}

func NewConnection(urls []string) (*Connection, error) {
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

	c := Connection{
		url:  url,
		conn: conn,
		quit: make(chan struct{}),
	}
	go c.handleConnectionFailures(healthCheckInterval)

	return &c, nil
}
