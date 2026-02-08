package imagefetcher

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type ImageData struct {
	ImageType string `json:"image_type"`
	FilePath  string `json:"file_path"`
}

type Response struct {
	Status  string      `json:"status"`
	Images  []ImageData `json:"images"`
	Message string      `json:"message"`
}

func FetchImages(objectID string) ([]ImageData, error) {
	now := time.Now()
	postUrl := fmt.Sprintf("http://imageutil:8000/fetch_stamps/%s", objectID)
	resp, err := http.Post(postUrl, "application/json", nil)

	if err != nil {
		return nil, fmt.Errorf("[%s] FETCH_IMG_ERR : %v \n", now, err)
	}

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("[%s] UNMARSHALLING_ERR : %v \n", now, err)
	}

	var result Response
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, fmt.Errorf("[%s] JSON_PARSE_ERR: %v", now, err)
	}
	if result.Status == "error" {
		return nil, fmt.Errorf("[%s] FETCH_IMG_ERR : %s\n", now, result.Message)
	}

	return result.Images, nil
}
