package eventstore

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/nivedita-verma/event-processor/pkg/eventspec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

func Test_NewDynamoDBStore(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	client := &dynamodb.Client{}
	tableName := "testTable"
	store := NewDynamoDBStore(client, tableName, logger)

	assert.NotNil(t, store)
	assert.Equal(t, client, store.client)
	assert.Equal(t, tableName, store.tableName)
	assert.Equal(t, logger, store.logger)
}

type mockDynamoDBClient struct {
	mock.Mock
}

func (m *mockDynamoDBClient) PutItem(ctx context.Context, input *dynamodb.PutItemInput, opts ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	args := m.Called(ctx, input)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*dynamodb.PutItemOutput), nil

}
func Test_DynamoDBStore_Persist(t *testing.T) {
	logger := zap.NewNop().Sugar()
	defer logger.Sync()

	t.Run("when error occurs during marshalling", func(t *testing.T) {
		client := &mockDynamoDBClient{}
		tableName := "testTable"
		store := NewDynamoDBStore(client, tableName, logger)
		store.marshal = func(interface{}) (map[string]types.AttributeValue, error) {
			return nil, assert.AnError
		}
		event := eventspec.Event{}
		err := store.Persist(context.Background(), event)

		t.Run("should return the marshalling error", func(t *testing.T) {
			assert.Error(t, err)
		})
	})

	t.Run("when PutItem returns an error", func(t *testing.T) {
		client := &mockDynamoDBClient{}
		tableName := "testTable"
		store := NewDynamoDBStore(client, tableName, logger)
		event := eventspec.Event{
			EventID:  "1",
			ClientID: "client-1",
			Type:     "TestEvent",
			Data: map[string]interface{}{
				"key": "value",
			},
		}
		item, err := store.marshal(event)
		assert.NoError(t, err)

		client.On("PutItem", mock.Anything, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      item,
		}).Return(nil, assert.AnError)

		err = store.Persist(context.Background(), event)

		client.AssertExpectations(t)
		t.Run("should return the PutItem error", func(t *testing.T) {
			assert.Error(t, err)
		})

	})

	t.Run("when PutItem is successful", func(t *testing.T) {
		client := &mockDynamoDBClient{}
		tableName := "testTable"
		store := NewDynamoDBStore(client, tableName, logger)
		event := eventspec.Event{
			EventID:  "1",
			ClientID: "client-1",
			Type:     "TestEvent",
			Data: map[string]interface{}{
				"key": "value",
			},
		}
		item, err := store.marshal(event)
		assert.NoError(t, err)

		client.On("PutItem", mock.Anything, &dynamodb.PutItemInput{
			TableName: &tableName,
			Item:      item,
		}).Return(&dynamodb.PutItemOutput{}, nil)

		err = store.Persist(context.Background(), event)

		client.AssertExpectations(t)

		t.Run("should complete without error", func(t *testing.T) {
			assert.NoError(t, err)
		})

	})
}
