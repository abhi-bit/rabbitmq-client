package rabbitmq

type Config struct {
	URLs         []string
	Exchange     string
	ExchangeType string

	PublisherConfirm bool

	QueueName   string
	BindingKeys []string

	DeadLetterExchange   string
	DeadLetterQueue      string
	DeadLetterRoutingKey string
}
