package pubsub

import amqp "github.com/rabbitmq/amqp091-go"

type SimpleQueueType int

const (
	Durable = iota
	Transient
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // SimpleQueueType is an "enum" type I made to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, nil
	}

	durable := true
	autoDelete, exclusive := false, false
	if queueType == 1 {
		durable = false
		autoDelete = true
		exclusive = true
	}

	queue, err := ch.QueueDeclare(
		queueName,
		durable,
		autoDelete,
		exclusive,
		false,
		amqp.Table{
			"x-dead-letter-exchange": "peril_dlx",
		},
	)

	return ch, queue, ch.QueueBind(queueName, key, exchange, false, nil)
}
