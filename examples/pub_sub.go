package main

import (
	"context"
	"log"
	"sync"

	"github.com/abhi-bit/rabbitmq"
)

func config() *rabbitmq.Config {
	return &rabbitmq.Config{
		URLs:          []string{"amqp://guest:guest@localhost:5672/"},
		Exchange:      "sample-ex",
		ExchangeType:  "topic",
		QueueName:     "sample-queue",
		PrefetchCount: 50,
		BindingKeys:   []string{"sample"},
	}
}

const (
	producerCount = 8
	consumeCount  = 1
)

func sample(_ []byte) error {
	return nil
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var wg sync.WaitGroup

	c, err := rabbitmq.NewConsumer(config())
	if err != nil {
		log.Fatal(err)
	}

	wg.Add(consumeCount)
	for i := 0; i < consumeCount; i++ {
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()
			log.Printf("Consumer ID: %d spawned\n", i)

			_, err := c.Consume(context.Background(), sample)
			if err != nil {
				log.Fatal(err)
				return
			}
		}(&wg, i)
	}

	wg.Add(producerCount)
	for i := 0; i < producerCount; i++ {
		go func(wg *sync.WaitGroup, i int) {
			defer wg.Done()

			p, err := rabbitmq.NewPublisher(config())
			if err != nil {
				log.Fatal(err)
			}

			log.Printf("Publisher ID: %d spawned\n", i)
			for {
				err := p.Publish([]byte("sample message"), []string{"sample"})
				if err != nil {
					log.Println(err)
				}
			}
		}(&wg, i)
	}

	wg.Wait()
}
