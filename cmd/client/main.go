package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"

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

	pauseQueueName := routing.PauseKey + "." + userName
	moveQueueName := routing.ArmyMovesPrefix + "." + userName
	warQueueKey := routing.WarRecognitionsPrefix + ".*"

	gameState := gamelogic.NewGameState(userName)

	if err := pubsub.SubscribeJSON(
		rabMQcon,
		routing.ExchangePerilDirect,
		pauseQueueName,
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
		moveQueueName,
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
		routing.WarRecognitionsPrefix,
		warQueueKey,
		pubsub.Durable,
		handlerWar(gameState, rabMQChan),
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
			if len(words) < 2 {
				fmt.Println("No amount of spam messages indicated...")
				continue
			}
			nMsg, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("Error converting str to int")
				continue
			}
			for range nMsg {
				msg := gamelogic.GetMaliciousLog()
				if err = publishGameLog(
					rabMQChan,
					userName,
					msg,
				); err != nil {
					fmt.Printf("error publishing malicious message: %v", err)
					continue
				}
			}

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Command not valid...")
			continue
		}
	}
}
