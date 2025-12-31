package internal

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type lasairConsumer struct {
	kafkaConsumer *kafka.Consumer
}

type ZTFSentinel struct {
	eventLog       string
	errorLog       string
	lasairConsumer *lasairConsumer
}

func NewZTFSentinel(evntLog string, errLog string, consumer *lasairConsumer) (*ZTFSentinel, error) {

	sentinel := ZTFSentinel{
		eventLog:       evntLog,
		errorLog:       errLog,
		lasairConsumer: consumer,
	}

	// open event & err logs
	errFile, err := os.Open(sentinel.errorLog)
	if err != nil {
		panic(err)
	}
	evntFile, err := os.Open(sentinel.eventLog)
	if err != nil {
		errFile.WriteString("Error opening event log....")
		panic(err)
	}

	defer evntFile.Close()
	defer errFile.Close()

	evntFile.WriteString("Initializing ZTF Sentinel....")

}

func NewLasairConsumer(host, groupId, topic string) (*lasairConsumer, error) {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": host,
		"group.id":          groupId,
	})

	if err != nil {
		return nil, err
	}

	err = c.Subscribe(topic, nil)

	if err != nil {
		return nil, err
	}

	return &lasairConsumer{
		kafkaConsumer: c,
	}, err
}

func (lc *lasairConsumer) Poll(timeoutMs int) kafka.Event {
	event := lc.kafkaConsumer.Poll(timeoutMs)
	return event
}

func (lc *lasairConsumer) Close() error {
	err := lc.kafkaConsumer.Close()

	if err != nil {
		return err
	}

	return nil
}
