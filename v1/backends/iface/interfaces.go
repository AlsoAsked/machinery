package iface

import (
	"context"

	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// Backend - a common interface for all result backends
type Backend interface {
	// Group related functions
	InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error
	GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error)
	GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error)
	TriggerChord(ctx context.Context, groupUUID string) (bool, error)

	// Setting / getting task state
	SetStatePending(ctx context.Context, signature *tasks.Signature) error
	SetStateReceived(ctx context.Context, signature *tasks.Signature) error
	SetStateStarted(ctx context.Context, signature *tasks.Signature) error
	SetStateRetry(ctx context.Context, signature *tasks.Signature) error
	SetStateSuccess(ctx context.Context, signature *tasks.Signature, results []*tasks.TaskResult) error
	SetStateFailure(ctx context.Context, signature *tasks.Signature, err string) error
	GetState(ctx context.Context, taskUUID string) (*tasks.TaskState, error)

	// Purging stored stored tasks states and group meta data
	IsAMQP() bool
	PurgeState(ctx context.Context, taskUUID string) error
	PurgeGroupMeta(ctx context.Context, groupUUID string) error
}

type DynamoDBAPI interface {
	PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
	UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error)
}
