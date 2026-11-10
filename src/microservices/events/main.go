package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var producer *kafka.Client

type Event struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}

type EventResponse struct {
	Status    string `json:"status"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Event     Event  `json:"event"`
}

func main() {
	brokers := os.Getenv("KAFKA_BROKERS")
	if brokers == "" {
		brokers = "kafka:9092"
	}
	addresses := strings.Split(brokers, ",")
	producer = &kafka.Client{Addr: kafka.TCP(addresses...), Timeout: 5 * time.Second}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: addresses, GroupID: "cinemaabyss-events",
		GroupTopics: []string{"movie-events", "user-events", "payment-events"},
		MinBytes:    1, MaxBytes: 1024 * 1024, StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()
	go consumeEvents(reader)

	http.HandleFunc("GET /api/events/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"status": true})
	})
	for _, kind := range []string{"movie", "user", "payment"} {
		http.HandleFunc("POST /api/events/"+kind, func(w http.ResponseWriter, r *http.Request) {
			createEvent(w, r, kind)
		})
	}

	port := os.Getenv("PORT")
	if port == "" {
		port = "8082"
	}
	log.Printf("Starting events microservice on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func createEvent(w http.ResponseWriter, r *http.Request, kind string) {
	var payload json.RawMessage
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid JSON"})
		return
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Expected one JSON object"})
		return
	}
	if err := validatePayload(kind, payload); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}

	var id [16]byte
	if _, err := rand.Read(id[:]); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Could not generate event ID"})
		return
	}
	event := Event{ID: fmt.Sprintf("%x", id), Type: kind, Timestamp: time.Now().UTC(), Payload: payload}
	data, err := json.Marshal(event)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// Each MVP topic has one partition, as configured in docker-compose.yml.
	result, err := producer.Produce(r.Context(), &kafka.ProduceRequest{
		Topic: kind + "-events", Partition: 0, RequiredAcks: kafka.RequireAll,
		Records: kafka.NewRecordReader(kafka.Record{
			Key: kafka.NewBytes([]byte(event.ID)), Value: kafka.NewBytes(data), Time: event.Timestamp,
		}),
	})
	if err == nil {
		err = result.Error
	}
	if err != nil {
		log.Printf("Failed to publish event: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Could not publish event to Kafka"})
		return
	}
	log.Printf("Published event: topic=%s-events partition=0 offset=%d event_id=%s", kind, result.BaseOffset, event.ID)
	writeJSON(w, http.StatusCreated, EventResponse{
		Status: "success", Partition: 0, Offset: result.BaseOffset, Event: event,
	})
}

func validatePayload(kind string, payload json.RawMessage) error {
	var p struct {
		MovieID     *int64    `json:"movie_id"`
		PaymentID   *int64    `json:"payment_id"`
		UserID      *int64    `json:"user_id"`
		Title       string    `json:"title"`
		Action      string    `json:"action"`
		Amount      *float64  `json:"amount"`
		Status      string    `json:"status"`
		Timestamp   time.Time `json:"timestamp"`
		Username    string    `json:"username"`
		Email       string    `json:"email"`
		Rating      float64   `json:"rating"`
		Genres      []string  `json:"genres"`
		Description string    `json:"description"`
		MethodType  string    `json:"method_type"`
	}
	if err := json.Unmarshal(payload, &p); err != nil {
		return err
	}
	valid := false
	switch kind {
	case "movie":
		valid = p.MovieID != nil && p.Title != "" && p.Action != ""
	case "user":
		valid = p.UserID != nil && p.Action != "" && !p.Timestamp.IsZero()
	case "payment":
		valid = p.PaymentID != nil && p.UserID != nil && p.Amount != nil && p.Status != "" && !p.Timestamp.IsZero()
	}
	if !valid {
		return fmt.Errorf("missing required %s event fields", kind)
	}
	return nil
}

func consumeEvents(reader *kafka.Reader) {
	ctx := context.Background()
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Failed to read event: %v", err)
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Consumed event: topic=%s partition=%d offset=%d event=%s", message.Topic, message.Partition, message.Offset, message.Value)
		if err := reader.CommitMessages(ctx, message); err != nil {
			log.Printf("Failed to commit event offset: %v", err)
		}
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(value)
}
