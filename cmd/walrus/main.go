package main

import (
	"github.com/mooyg/walrus/internal/broker"
	logger "github.com/mooyg/walrus/internal/log"
)

func main() {
	logger.Init("debug")

	b, err := broker.NewBroker()

	if err != nil {
		return
	}

	b.CreateTopic("test")
}
