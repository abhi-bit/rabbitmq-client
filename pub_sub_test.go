package rabbitmq

import (
	"fmt"
	"golang.org/x/net/context"
	"log"
	"sync"
	"testing"
)

const (
	expectedCount = int64(10)
)

func config() *Config {
	return &Config{
		URLs:         []string{"amqp://guest:guest@localhost:5672/"},
		Exchange:     "pub_sub_test",
		ExchangeType: "topic",

		QueueName:   "pub_sub_queue_test",
		BindingKeys: []string{"testing"},

		DeadLetterExchange: "dead-letter",
		DeadLetterQueue:    "dead-letter-queue",
	}
}

func process(msg []byte) error {
	return nil
}

func errorFunc(msg []byte) error {
	return fmt.Errorf("could not process")
}

func PublisherAndConsumer(t *testing.T) (*Publisher, *Consumer, error) {
	conf := config()
	c, err := NewConsumer(conf)
	if err != nil {
		t.Error(err)
		return nil, nil, err
	}

	p, err := NewPublisher(conf)
	if err != nil {
		t.Error(err)
		return nil, c, err
	}

	return p, c, nil
}

// TODO: test with producer before consumer on exchange
// TODO: Cleanup all exchanges and queues after test run
func TestPubSub(t *testing.T) {
	p, c, err := PublisherAndConsumer(t)
	if err != nil {
		return
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	defer c.Close()
	defer p.Close()

	var actualCount int64
	log.Println()

	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()

		ctx, cancel := context.WithCancel(context.Background())
		actualCount, err = c.Consume(ctx, process)
		if err != nil {
			t.Error(err)
			return
		}
		log.Println()

		if actualCount == expectedCount {
			cancel()
		}

	}(&wg)

	for i := int64(0); i < expectedCount; i++ {
		err = p.Publish([]byte("sample message"), "testing")
		log.Println()
		if err != nil {
			t.Error(err)
			return
		}
	}
	p.Close()
	wg.Wait()

	if actualCount != expectedCount {
		log.Println()
		t.Errorf("Expected count: %d actual count: %d", expectedCount, actualCount)
	}

}

/*func TestDeadLetter(t *testing.T) {
	conf := config()
	err := DeadLetterExchange(conf)
	if err != nil {
		t.Error(err)
	}

	c, err := NewConsumer(conf)
	if err != nil {
		t.Error(err)
	}
	defer c.Close()

	go func() {
		// _, err = c.Consume(errorFunc)
		_, err = c.Consume(process)
		if err != nil {
			t.Error(err)
		}
	}()

	p, err := NewPublisher(conf)
	if err != nil {
		t.Error(err)
	}
	defer p.Close()

	err = p.Publish([]byte("Some message."), "testing")
	if err != nil {
		t.Error(err)
	}
}*/

/*func BenchmarkPubSub(b *testing.B) {
	conf := config()
	c, err := NewConsumer(conf)
	if err != nil {
		b.Error(err)
	}
	defer c.Close()

	go func() {
		err = c.Consume(process)
		if err != nil {
			b.Error(err)
		}
	}()

	p, err := NewPublisher(conf)
	if err != nil {
		b.Error(err)
	}

	defer p.Close()

	msg := []byte("sample message")

	for i := 0; i < b.N; i++ {
		err = p.Publish(msg, "testing")
		if err != nil {
			b.Error(err)
		}
	}
}*/
