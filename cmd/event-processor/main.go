package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/nivedita-verma/event-processor/internal/app/eventprocessor"
	"github.com/nivedita-verma/event-processor/internal/pkg/eventstore"
	"github.com/nivedita-verma/event-processor/internal/pkg/vars"
	"go.uber.org/zap"
)

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		panic(err)
	}
	defer logger.Sync()

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		panic(err)
	}
	client := dynamodb.NewFromConfig(cfg)
	store := eventstore.NewDynamoDBStore(client, os.Getenv(vars.TableNameEnvVar), logger.Sugar())
	handler := eventprocessor.NewHandler(logger.Sugar(), eventprocessor.NewService(store))
	lambda.Start(handler.HandleSQSEvent)
}
