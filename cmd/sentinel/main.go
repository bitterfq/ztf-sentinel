package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/bitterfq/ztf-sentinel/internal/consumer"
	"github.com/bitterfq/ztf-sentinel/internal/database/db"
	"github.com/bitterfq/ztf-sentinel/internal/repository"
	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

func main() {

	_ = godotenv.Load()
	dbConn, err := sql.Open("postgres", os.Getenv("DBCONN"))
	if err != nil {
		log.Fatal(err)
	}
	queries := db.New(dbConn)

	kafkaConsumer, err := consumer.NewLasairConsumer(
		os.Getenv("HOST"),
		os.Getenv("GROUP_ID"),
		os.Getenv("TOPIC"),
	)

	if err != nil {
		log.Fatal(err)
	}

	sentinel, err := consumer.NewZTFSentinel(
		"logs/event.log",
		"logs/error.log",
		kafkaConsumer,
		repository.NewPostgresRepo(queries),
	)

	if err != nil {
		log.Fatal(err)
	}

	sentinel.Run()
}
