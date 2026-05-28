package pubsub

import (
	"bytes"
	"encoding/gob"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	if err = ch.Qos(10, 0, false); err != nil {
		return err
	}

	deliveryChan, err := ch.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range deliveryChan {
			var payload T

			buff := bytes.NewBuffer(msg.Body)
			decoder := gob.NewDecoder(buff)

			if err = decoder.Decode(&payload); err != nil {
				continue
			}

			ackType := handler(payload)

			switch ackType {
			case 0:
				err = msg.Ack(true)
				fmt.Print("Message Acknoledged\n> ")
			case 1:
				err = msg.Nack(false, true)
				fmt.Print("Message Negatve Acknoledged - Requeued\n> ")
			case 2:
				err = msg.Nack(false, false)
				fmt.Print("Message Negatve Acknoledged - Discarded\n> ")
			}
		}
	}()
	return err
}
