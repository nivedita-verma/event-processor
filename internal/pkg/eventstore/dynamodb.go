package eventstore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
	"go.uber.org/zap"
)

type DynamoDBStore struct {
	client    dynamoDBAPI
	tableName string
	marshal   func(interface{}) (map[string]types.AttributeValue, error)
	logger    *zap.SugaredLogger
}

type dynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
}

func NewDynamoDBStore(api dynamoDBAPI, tableName string, logger *zap.SugaredLogger) *DynamoDBStore {
	return &DynamoDBStore{
		client:    api,
		tableName: tableName,
		marshal:   attributevalue.MarshalMap,
		logger:    logger,
	}
}

func (s *DynamoDBStore) Persist(ctx context.Context, event eventspec.Event) error {
	item, err := s.marshal(event)
	if err != nil {
		return err
	}

	s.logger.Infof("Persisting event to DynamoDB: %+v", event)
	s.logger.Infof("Persisting item to DynamoDB: %+v", item)

	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.tableName),
		Item:      item,
	})
	if err != nil {
		return err
	}
	return nil
}
