package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
)

func main() {
	err := godotenv.Load("eventsimulator.env")
	if err != nil {
		log.Fatalf("Error loading eventsimulator.env file: %v", err)
	}

	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		log.Fatal("QUEUE_URL must be set in eventsimulator.env")
	}

	rateMs, _ := strconv.Atoi(getEnv("RATE_MS", "500"))
	validRatio, _ := strconv.Atoi(getEnv("VALID_RATIO", "80"))
	totalEvents, _ := strconv.Atoi(getEnv("TOTAL_EVENTS", "0")) // 0 means run indefinitely

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load AWS SDK config: %v", err)
	}
	client := sqs.New(sqs.Options{
		Region:           cfg.Region,
		Credentials:      cfg.Credentials,
		EndpointResolver: sqs.EndpointResolverFromURL(queueURL),
	})

	ticker := time.NewTicker(time.Duration(rateMs) * time.Millisecond)
	defer ticker.Stop()

	log.Printf("Simulator started → queue=%s, interval=%dms, validRatio=%d%%",
		queueURL, rateMs, validRatio)

	sent := 0
	for range ticker.C {
		isValid := rand.Intn(100) < validRatio

		var msgBody string
		if isValid {
			event := eventspec.Event{
				EventID:  uuid.NewString(),
				ClientID: fmt.Sprintf("client-%d", rand.Intn(5)+1),
				Type:     string(eventspec.ValidEventTypes[rand.Intn(len(eventspec.ValidEventTypes))]),
				Data: map[string]interface{}{
					"value": rand.Intn(1000),
					"time":  time.Now().String(),
				},
			}
			b, _ := json.Marshal(event)
			msgBody = string(b)
		} else {
			ev := invalidEventGenerator()
			b, _ := json.Marshal(ev)
			msgBody = string(b)
		}

		_, err := client.SendMessage(context.TODO(), &sqs.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String(msgBody),
		})
		if err != nil {
			log.Printf("failed to send message: %v", err)
		} else {
			log.Printf("Sent event (valid=%t): %s", isValid, msgBody)
		}

		sent++
		if totalEvents > 0 && sent >= totalEvents {
			log.Printf("Reached totalEvents=%d, stopping simulator.", totalEvents)
			break
		}
	}
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func invalidEventGenerator() eventspec.Event {
	invalidEvents := []eventspec.Event{
		// Invalid event type
		{EventID: uuid.NewString(), ClientID: fmt.Sprintf("client-%d", rand.Intn(5)+1), Type: "Invalid", Data: map[string]interface{}{}},
		// Missing required fields
		{EventID: "", ClientID: fmt.Sprintf("client-%d", rand.Intn(5)+1), Type: "monitoringAlert", Data: map[string]interface{}{}},
		{EventID: uuid.NewString(), ClientID: "", Type: "notification", Data: map[string]interface{}{}},
		{EventID: uuid.NewString(), ClientID: uuid.NewString(), Type: "transaction", Data: nil},
		// Empty Event
		{},
	}
	return invalidEvents[rand.Intn(len(invalidEvents))]
}
