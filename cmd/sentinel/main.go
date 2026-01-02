package main

import (
	"log"

	"github.com/bitterfq/ztf-sentinel/internal"
)

func main() {
	consumer, err := internal.NewLasairConsumer(
		"kafka.lsst.ac.uk:9092",
		"initialTest1",
		"lasair_1568BrightFastTransients",
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
