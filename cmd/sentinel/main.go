package main

import (
	"log"
	"os"

	"github.com/bitterfq/ztf-sentinel/internal"
	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	consumer, err := internal.NewLasairConsumer(
		os.Getenv("HOST"),
		os.Getenv("GROUP_ID"),
		os.Getenv("TOPIC"),
	)

	if err != nil {
		log.Fatal(err)
	}

	sentinel, err := internal.NewZTFSentinel(
		"logs/event.log",
		"logs/error.log",
		consumer,
	)

	if err != nil {
		log.Fatal(err)
	}

	sentinel.Run()
}
