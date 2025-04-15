package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/segmentio/kafka-go"
)

// CampaignData represents the structure of the incoming campaign data
type CampaignData struct {
	CampaignID  string  `json:"campaign_id"`
	Platform    string  `json:"platform"`
	Timestamp   int64   `json:"timestamp"`
	Impressions int     `json:"impressions"`
	Clicks      int     `json:"clicks"`
	Conversions int     `json:"conversions"`
	Cost        float64 `json:"cost"`
	Revenue     float64 `json:"revenue"`
}

// KafkaWriter is a global Kafka writer instance
var KafkaWriter *kafka.Writer

func initKafkaWriter() {
	KafkaWriter = &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092"),
		Topic:    "campaign-data",
		Balancer: &kafka.LeastBytes{},
	}
}

func ingestHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var data CampaignData
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&data); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}

	// Basic validation
	if data.CampaignID == "" || data.Platform == "" || data.Timestamp == 0 {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Serialize data to JSON
	msgBytes, err := json.Marshal(data)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	// Produce message to Kafka
	err = KafkaWriter.WriteMessages(r.Context(),
		kafka.Message{
			Key:   []byte(data.CampaignID),
			Value: msgBytes,
		},
	)
	if err != nil {
		http.Error(w, "Failed to write to Kafka", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusAccepted)
	w.Write([]byte("Data ingested successfully"))
}

func main() {
	initKafkaWriter()
	defer KafkaWriter.Close()

	http.HandleFunc("/ingest", ingestHandler)

	srv := &http.Server{
		Addr:         ":8080",
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	log.Println("Starting server on :8080")
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
