package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlerLog(log routing.GameLog) pubsub.AckType {
	defer fmt.Print("> ")
	if err := gamelogic.WriteLog(log); err != nil {
		return pubsub.NackDiscard
	}
	return pubsub.Ack
}
