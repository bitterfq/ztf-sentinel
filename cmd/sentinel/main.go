package main

import (
	"database/sql"
	"log"
	"os"

	"github.com/bitterfq/ztf-sentinel/internal/consumer"
	"github.com/bitterfq/ztf-sentinel/internal/database/db"
	imagefetcher "github.com/bitterfq/ztf-sentinel/internal/image_fetcher"
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

	repo := repository.NewPostgresRepo(queries)

	imgFetcher := imagefetcher.NewImageFetcher(
		5,
		"http://imageutil:8000",
		50,
		repo,
	)

	sentinel, err := consumer.NewZTFSentinel(
		"logs/event.log",
		"logs/error.log",
		kafkaConsumer,
		repo,
		imgFetcher,
	)

	if err != nil {
		log.Fatal(err)
	}

	imgFetcher.Start()
	sentinel.Run()
}
