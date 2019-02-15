package rabbitmq

func DeadLetterExchange(conf *Config) error {
	conn, err := NewConnection(conf.URLs)
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(
		conf.DeadLetterExchange,
		"fanout", // routing type
		true,     // durable
		false,    // auto delete
		false,    // internal
		false,    // no wait
		nil); err != nil {
		return err
	}

	q, err := ch.QueueDeclare(
		conf.DeadLetterQueue,
		true,
		false,
		false,
		false,
		nil)
	if err != nil {
		return err
	}

	return ch.QueueBind(
		q.Name,
		"",
		conf.DeadLetterExchange,
		false,
		nil)
}
