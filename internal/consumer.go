package internal

import (
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LasairConsumer struct {
	kafkaConsumer *kafka.Consumer
}

type ZTFSentinel struct {
	eventLog       *os.File
	errorLog       *os.File
	LasairConsumer *LasairConsumer
}

func NewZTFSentinel(evntLog string, errLog string, consumer *LasairConsumer) (*ZTFSentinel, error) {

	sentinel := ZTFSentinel{
		eventLog:       nil,
		errorLog:       nil,
		LasairConsumer: consumer,
	}

	// open event & err logs
	var err error

	sentinel.errorLog, err = os.OpenFile(errLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("FAILED TO OPEN ERROR LOG FILE: %w", err)
	}
	sentinel.eventLog, err = os.OpenFile(evntLog, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("FAILED TO OPEN EVENT LOG FIL: %w", err)
	}

	sentinel.eventLog.WriteString("🪐 Initializing ZTF Sentinel....\n")

	return &sentinel, nil
}

func NewLasairConsumer(host, groupId, topic string) (*LasairConsumer, error) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          groupId,
	})

	if err != nil {
		return nil, fmt.Errorf("Failed to Initialize Kafka Consumer: %w", err)
	}

	err = c.Subscribe(topic, nil)
	if err != nil {
		return nil, fmt.Errorf("Failed to Subscribe to Kafka topic: %w", err)
	}

	return &LasairConsumer{
		kafkaConsumer: c,
	}, err
}

func (lc *LasairConsumer) Poll(timeoutMs int) kafka.Event {
	event := lc.kafkaConsumer.Poll(timeoutMs)
	return event
}

func (lc *LasairConsumer) Close() error {
	return lc.kafkaConsumer.Close()
}

func (sentinel *ZTFSentinel) CloseFiles() error {
	err := sentinel.eventLog.Close()
	if err != nil {
		panic(err)
	}
	err = sentinel.errorLog.Close()
	if err != nil {
		panic(err)
	}

	return nil
}

func (sentinel *ZTFSentinel) Run() {

	for {
		// TODO: add flag to pass in user defined timeout args
		msg := sentinel.LasairConsumer.Poll(3600)
		now := time.Now().UTC().Format(time.RFC3339)

		switch e := msg.(type) {
		case kafka.Error:
			fmt.Fprintf(sentinel.errorLog, "[%s] %s\n", now, msg.String())

		case *kafka.Message:
			alert := e.Value
			fmt.Fprintf(sentinel.eventLog, "[%s] %s\n", now, string(e.Value))
			fmt.Println(alert)

		case nil:
			message := fmt.Sprintf("[%s] Poll timeout - no new messages\n", now)
			sentinel.eventLog.WriteString(message)

		}

	}

}
