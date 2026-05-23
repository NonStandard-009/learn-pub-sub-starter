package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	conStr := "amqp://guest:guest@localhost:5672/"
	rabMQcon, err := amqp.Dial(conStr)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}
	defer rabMQcon.Close()
	fmt.Println("Successful SERVER connetion to RabbitMQ")

	ch, err := rabMQcon.Channel()
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(
		rabMQcon,
		routing.ExchangePerilTopic,
		"game_logs",
		"game_logs.*",
		pubsub.Durable,
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
		case "quit":
			fmt.Println("Sending quit message...")
			return
		default:
			fmt.Println("Command not valid...")
		}

		if err != nil {
			fmt.Printf("unexpected error: %v", err)
			return
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
