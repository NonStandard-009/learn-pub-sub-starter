package main

import (
	"fmt"
	"os"
	"os/signal"

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
	fmt.Println("Successful connetion to RabbitMQ")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
