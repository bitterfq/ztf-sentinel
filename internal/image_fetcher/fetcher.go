package imagefetcher

import (
	"context"
	"fmt"
	"sync"

	"github.com/bitterfq/ztf-sentinel/internal/domains"
	"github.com/bitterfq/ztf-sentinel/internal/repository"
)

type ImageFetcher struct {
	workers int
	wg      sync.WaitGroup
	jobs    chan Job
	postUrl string
	repo    repository.ImageRepository
}

func NewImageFetcher(workerCount int, postUrl string, queueSize int, repo repository.ImageRepository) *ImageFetcher {
	return &ImageFetcher{
		workers: workerCount,
		jobs:    make(chan Job, queueSize),
		postUrl: postUrl,
		repo:    repo,
	}
}

func (imgFetcher *ImageFetcher) Start() {
	for i := 0; i < imgFetcher.workers; i++ {
		imgFetcher.wg.Add(1)
		go imgFetcher.worker()
	}
}

func (imgFetcher *ImageFetcher) worker() {
	defer imgFetcher.wg.Done()
	ctx := context.Background()

	for job := range imgFetcher.jobs {
		imageData, err := FetchImages(job.ObjectID)
		if err != nil {
			fmt.Printf("Failed to fetch images for %s: %v\n", job.ObjectID, err)
			continue
		}

		for _, data := range imageData {
			var image domains.Image
			image.AlertID = job.AlertID
			image.ImageType = data.ImageType
			image.FilePath = data.FilePath
			imgFetcher.repo.SaveImage(ctx, image)
		}
	}
}

func (imgFetcher *ImageFetcher) Enqueue(job Job) {
	imgFetcher.jobs <- job
}

func (imgFetcher *ImageFetcher) Stop() {
	close(imgFetcher.jobs)
	imgFetcher.wg.Wait()
}
