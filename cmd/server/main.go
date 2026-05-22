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

	gamelogic.PrintServerHelp()
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}
		if words[0] == "pause" {
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
		} else if words[0] == "resume" {
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
		} else if words[0] == "quit" {
			fmt.Println("Sending quit message...")
			return
		} else {
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
