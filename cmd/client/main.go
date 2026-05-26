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

	rabMQChan, err := rabMQcon.Channel()
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	userName, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	pauseQueue := routing.PauseKey + "." + userName
	moveQueue := routing.ArmyMovesPrefix + "." + userName
	warQueueKey := routing.WarRecognitionsPrefix + ".*"

	gameState := gamelogic.NewGameState(userName)

	if err := pubsub.SubscribeJSON(
		rabMQcon,
		routing.ExchangePerilDirect,
		pauseQueue,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	); err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	if err := pubsub.SubscribeJSON(
		rabMQcon,
		routing.ExchangePerilTopic,
		moveQueue,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState, rabMQChan),
	); err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	if err := pubsub.SubscribeJSON(
		rabMQcon,
		routing.ExchangePerilTopic,
		"war",
		warQueueKey,
		pubsub.Durable,
		handlerWar(gameState),
	); err != nil {
		fmt.Printf("unexpected error: %v", err)
		return
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		<-signalChan
		fmt.Println("Exiting program due to admin input...")
		os.Exit(0)
	}()

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Printf("ERROR: %v\n", err)
				continue
			}

		case "move":
			armyMove, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
				continue
			}
			fmt.Printf("Successful move to %s\n", armyMove.ToLocation)

			err = pubsub.PublishJSON(
				rabMQChan,
				routing.ExchangePerilTopic,
				routing.ArmyMovesPrefix+"."+armyMove.Player.Username,
				armyMove,
			)
			if err != nil {
				fmt.Printf("publish error: %v\n", err)
				continue
			}
			fmt.Println("Move published successfully")

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
			continue
		}
	}
}
