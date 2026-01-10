package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/bitterfq/ztf-sentinel/internal/domains"
	"github.com/bitterfq/ztf-sentinel/internal/repository"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type LasairConsumer struct {
	kafkaConsumer *kafka.Consumer
}

type ZTFSentinel struct {
	eventLog       *os.File
	errorLog       *os.File
	LasairConsumer *LasairConsumer
	repo           repository.AlertRepository
}

func NewZTFSentinel(evntLog string, errLog string, consumer *LasairConsumer, repo repository.AlertRepository) (*ZTFSentinel, error) {

	sentinel := ZTFSentinel{
		eventLog:       nil,
		errorLog:       nil,
		LasairConsumer: consumer,
		repo:           repo,
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
		"auto.offset.reset": "earliest",
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

	ctx := context.Background()
	for {
		// TODO: add flag to pass in user defined timeout args
		msg := sentinel.LasairConsumer.Poll(3600)
		now := time.Now().UTC().Format(time.RFC3339)

		switch e := msg.(type) {
		case kafka.Error:
			fmt.Fprintf(sentinel.errorLog, "[%s] KAFKA_ERR: %v\n", now, e)

		case *kafka.Message:

			// create domain type
			var alert domains.Alert

			// unmarshall data into domain type
			err := json.Unmarshal(e.Value, &alert)
			if err != nil {
				fmt.Fprintf(sentinel.errorLog, "[%s] UNMARSHAL_ERR: %v | %s\n", now, err, msg.String())
				continue
			}

			// for all that is not in the data packet, populate our alert type with
			alert.ID = fmt.Sprintf("%s_%f", alert.ObjectID, alert.LatestDetection)
			alert.ReceivedAt = time.Now()

			err = sentinel.repo.Save(ctx, alert)
			if err != nil {
				fmt.Fprintf(sentinel.errorLog, "[%s] SAVE_ERR: %v | %s\n", now, err, msg.String())
			}

		case nil:
			continue
		}

	}

}
