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
	fmt.Println("Starting Peril client...")
	conStr := "amqp://guest:guest@localhost:5672/"
	rabMQcon, err := amqp.Dial(conStr)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}
	defer rabMQcon.Close()
	fmt.Println("Successful CLIENT connetion to RabbitMQ")

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}
	queueName := routing.PauseKey + "." + userName

	_, _, err = pubsub.DeclareAndBind(
		rabMQcon,
		routing.ExchangePerilDirect,
		queueName,
		routing.PauseKey,
		pubsub.Transient,
	)
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	gameState := gamelogic.NewGameState(userName)
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Printf("unexpected error: %v", err)
				return
			}
		case "move":
			aM, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("unexpected error: %v", err)
				return
			}
			fmt.Printf("Successful move to %s\n", aM.ToLocation)
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Println("Command not valid...")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

}
