package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"sync"
	"testing"
	"time"
)

const (
	deadLetterExchange = "dead-letter"
	deadLetterQueue    = "dead-letter-queue"

	bindingKey = "testing"
	exchange   = "pub_sub_test"
	queueName  = "pub_sub_queue_test"

	expectedRunCount = int64(10000)
)

var (
	actualRunCount = int64(0)
)

func cleanup() {
	deleteQueue(deadLetterQueue)
	deleteExchange(deadLetterExchange)
	deleteQueue(queueName)
	deleteExchange(exchange)
}

func config() *Config {
	return &Config{
		URLs:               []string{"amqp://guest:guest@localhost:5672/"},
		Exchange:           exchange,
		ExchangeType:       "topic",
		QueueName:          queueName,
		BindingKeys:        []string{bindingKey},
		DeadLetterExchange: deadLetterExchange,
		DeadLetterQueue:    deadLetterQueue,
	}
}

func process(_ []byte) error {
	actualRunCount++
	return nil
}

func errorFn(_ []byte) error {
	actualRunCount++
	return fmt.Errorf("could not process")
}

func deleteExchange(exchange string) error {
	cmd := exec.Command(
		"rabbitmqadmin",
		"delete",
		"exchange",
		fmt.Sprintf("name=%s", exchange))

	err := cmd.Run()
	if err != nil {
		return err
	}
	return cmd.Wait()
}

func deleteQueue(queue string) error {
	cmd := exec.Command(
		"rabbitmqctl",
		"delete_queue",
		queue)

	err := cmd.Run()
	if err != nil {
		return err
	}
	return cmd.Wait()
}

func PublisherAndConsumer() (*Publisher, *Consumer, error) {
	conf := config()
	c, err := NewConsumer(conf)
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}

	p, err := NewPublisher(conf)
	if err != nil {
		log.Fatal(err)
		return nil, c, err
	}

	return p, c, nil
}

func consume(fn processFn, c *Consumer, wg *sync.WaitGroup, expected int64) {
	defer wg.Done()
	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				if actualRunCount == expected {
					cancel()
					return
				} else {
					log.Println("actual run counter", actualRunCount)
				}
			}
		}
	}()

	_, err := c.Consume(ctx, fn)
	if err != nil {
		log.Fatal(err)
		return
	}
}

func TestPubSub(t *testing.T) {
	p, c, err := PublisherAndConsumer()
	if err != nil {
		return
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defer func() {
		actualRunCount = 0
		c.Close()
		p.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)

	go consume(process, c, &wg, expectedRunCount)

	for i := int64(0); i < expectedRunCount; i++ {
		err = p.Publish([]byte("sample message"), bindingKey)
		if err != nil {
			t.Error(err)
			return
		}
	}
	wg.Wait()

	if actualRunCount != expectedRunCount {
		t.Errorf("Expected count: %d actual count: %d", expectedRunCount, actualRunCount)
	}
}

func TestDeadLetter(t *testing.T) {
	p, c, err := PublisherAndConsumer()
	if err != nil {
		return
	}
	defer func() {
		actualRunCount = 0
		c.Close()
		p.Close()
	}()

	err = DeadLetterExchange(config())
	if err != nil {
		t.Error(err)
		return
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go consume(errorFn, c, &wg, expectedRunCount)

	for i := int64(0); i < expectedRunCount; i++ {
		err = p.Publish([]byte("sample message"), bindingKey)
		if err != nil {
			t.Error(err)
			return
		}
	}

	wg.Wait()
	if actualRunCount != expectedRunCount {
		t.Errorf("Expected count: %d actual count: %d", expectedRunCount, actualRunCount)
	}

	// config to consume from dead letter queue
	conf := config()
	conf.ExchangeType = "fanout"
	conf.Exchange = deadLetterExchange
	conf.QueueName = deadLetterQueue
	conf.DeadLetterExchange = ""

	dc, err := NewConsumer(conf)
	if err != nil {
		t.Error(err)
		return
	}

	wg.Add(1)
	go consume(process, dc, &wg, 2*expectedRunCount)
	wg.Wait()

	if actualRunCount != 2*expectedRunCount {
		t.Errorf("Expected count: %d actual count: %d", expectedRunCount, actualRunCount)
	}

}

func BenchmarkPub(b *testing.B) {
	p, c, err := PublisherAndConsumer()
	if err != nil {
		return
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defer func() {
		c.Close()
		p.Close()
	}()

	for i := 0; i < b.N; i++ {
		err = p.Publish([]byte("sample message"), bindingKey)
		if err != nil {
			b.Error(err)
		}
	}
}
