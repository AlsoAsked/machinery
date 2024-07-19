package dynamodb

import (
	"context"
	"errors"
	"os"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

var (
	TestDynamoDBBackend    *Backend
	TestErrDynamoDBBackend *Backend
	TestCnf                *config.Config
	TestDBClient           iface.DynamoDBAPI
	TestErrDBClient        iface.DynamoDBAPI
	TestGroupMeta          *tasks.GroupMeta
	TestTask1              map[string]types.AttributeValue
	TestTask2              map[string]types.AttributeValue
	TestTask3              map[string]types.AttributeValue
)

type TestDynamoDBClient struct {
	iface.DynamoDBAPI

	PutItemOverride      func(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	GetItemOverride      func(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	BatchGetItemOverride func(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error)
	UpdateItemOverride   func(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
}

func (t *TestDynamoDBClient) ResetOverrides() {
	t.PutItemOverride = nil
	t.UpdateItemOverride = nil
	t.GetItemOverride = nil
	t.BatchGetItemOverride = nil
}

func (t *TestDynamoDBClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	if t.PutItemOverride != nil {
		return t.PutItemOverride(ctx, params, optFns...)
	}
	return &dynamodb.PutItemOutput{}, nil
}

func (t *TestDynamoDBClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	if t.GetItemOverride != nil {
		return t.GetItemOverride(ctx, params, optFns...)
	}
	var output *dynamodb.GetItemOutput
	switch *params.TableName {
	case "group_metas":
		output = &dynamodb.GetItemOutput{
			Item: map[string]types.AttributeValue{
				"TaskUUIDs": &types.AttributeValueMemberL{
					Value: []types.AttributeValue{
						&types.AttributeValueMemberS{
							Value: "testTaskUUID1",
						},
						&types.AttributeValueMemberS{
							Value: "testTaskUUID2",
						},
						&types.AttributeValueMemberS{
							Value: "testTaskUUID3",
						},
					},
				},
				"ChordTriggered": &types.AttributeValueMemberBOOL{
					Value: false,
				},
				"GroupUUID": &types.AttributeValueMemberS{
					Value: "testGroupUUID",
				},
				"Lock": &types.AttributeValueMemberBOOL{
					Value: false,
				},
			},
		}
	case "task_states":
		if params.Key["TaskUUID"] == nil {
			output = &dynamodb.GetItemOutput{
				Item: map[string]types.AttributeValue{
					"Error": &types.AttributeValueMemberBOOL{
						Value: false,
					},
					"State": &types.AttributeValueMemberS{
						Value: tasks.StatePending,
					},
					"TaskUUID": &types.AttributeValueMemberS{
						Value: "testTaskUUID1",
					},
					"Results:": &types.AttributeValueMemberNULL{
						Value: true,
					},
				},
			}
		} else if attributeValue, ok := params.Key["TaskUUID"].(*types.AttributeValueMemberS); ok {
			if attributeValue.Value == "testTaskUUID1" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask1,
				}
			} else if attributeValue.Value == "testTaskUUID2" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask2,
				}

			} else if attributeValue.Value == "testTaskUUID3" {
				output = &dynamodb.GetItemOutput{
					Item: TestTask3,
				}
			}
		}
	}
	return output, nil
}

func (t *TestDynamoDBClient) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	if t.BatchGetItemOverride != nil {
		return t.BatchGetItemOverride(ctx, params, optFns...)
	}
	return &dynamodb.BatchGetItemOutput{}, nil
}

func (t *TestDynamoDBClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return &dynamodb.DeleteItemOutput{}, nil
}
func (t *TestDynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	if t.UpdateItemOverride != nil {
		return t.UpdateItemOverride(ctx, params, optFns...)
	}
	return &dynamodb.UpdateItemOutput{}, nil
}
func (t *TestDynamoDBClient) ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return &dynamodb.ListTablesOutput{
		TableNames: []string{
			"group_metas",
			"task_states",
		},
	}, nil
}

// Always returns error
type TestErrDynamoDBClient struct {
	iface.DynamoDBAPI
}

func (t *TestErrDynamoDBClient) PutItem(ctx context.Context, params *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	return nil, errors.New("error when putting an item")
}

func (t *TestErrDynamoDBClient) GetItem(ctx context.Context, params *dynamodb.GetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return nil, errors.New("error when getting an item")
}

func (t *TestErrDynamoDBClient) BatchGetItem(ctx context.Context, params *dynamodb.BatchGetItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.BatchGetItemOutput, error) {
	return nil, errors.New("error when getting batch items")
}

func (t *TestErrDynamoDBClient) DeleteItem(ctx context.Context, params *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	return nil, errors.New("error when deleting an item")
}

func (t *TestErrDynamoDBClient) UpdateItem(ctx context.Context, params *dynamodb.UpdateItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	return nil, errors.New("error when updating an item")
}

func (t *TestErrDynamoDBClient) ListTables(ctx context.Context, params *dynamodb.ListTablesInput, optFns ...func(*dynamodb.Options)) (*dynamodb.ListTablesOutput, error) {
	return nil, errors.New("error when listing tables")
}

func init() {
	TestCnf = &config.Config{
		ResultBackend:   os.Getenv("DYNAMODB_URL"),
		ResultsExpireIn: 30,
		DynamoDB: &config.DynamoDBConfig{
			TaskStatesTable: "task_states",
			GroupMetasTable: "group_metas",
		},
	}
	TestDBClient = new(TestDynamoDBClient)
	TestDynamoDBBackend = &Backend{cnf: TestCnf, client: TestDBClient}

	TestCnf.DynamoDB.Client = TestDBClient

	TestErrDBClient = new(TestErrDynamoDBClient)
	TestErrDynamoDBBackend = &Backend{cnf: TestCnf, client: TestErrDBClient}

	TestGroupMeta = &tasks.GroupMeta{
		GroupUUID: "testGroupUUID",
		TaskUUIDs: []string{"testTaskUUID1", "testTaskUUID2", "testTaskUUID3"},
	}
}

func (b *Backend) GetConfig() *config.Config {
	return b.cnf
}

func (b *Backend) GetClient() iface.DynamoDBAPI {
	return b.client
}

func (b *Backend) GetGroupMetaForTest(ctx context.Context, groupUUID string) (*tasks.GroupMeta, error) {
	return b.getGroupMeta(ctx, groupUUID)
}

func (b *Backend) UnmarshalGroupMetaGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.GroupMeta, error) {
	return b.unmarshalGroupMetaGetItemResult(result)
}

func (b *Backend) UnmarshalTaskStateGetItemResultForTest(result *dynamodb.GetItemOutput) (*tasks.TaskState, error) {
	return b.unmarshalTaskStateGetItemResult(result)
}

func (b *Backend) SetTaskStateForTest(ctx context.Context, taskState *tasks.TaskState) error {
	return b.setTaskState(ctx, taskState)
}

func (b *Backend) ChordTriggeredForTest(ctx context.Context, groupUUID string) error {
	return b.chordTriggered(ctx, groupUUID)
}

func (b *Backend) UpdateGroupMetaLockForTest(ctx context.Context, groupUUID string, status bool) error {
	return b.updateGroupMetaLock(ctx, groupUUID, status)
}

func (b *Backend) UnlockGroupMetaForTest(ctx context.Context, groupUUID string) error {
	return b.unlockGroupMeta(ctx, groupUUID)
}

func (b *Backend) LockGroupMetaForTest(ctx context.Context, groupUUID string) error {
	return b.lockGroupMeta(ctx, groupUUID)
}

func (b *Backend) GetStatesForTest(ctx context.Context, taskUUIDs ...string) ([]*tasks.TaskState, error) {
	return b.getStates(ctx, taskUUIDs)
}

func (b *Backend) UpdateToFailureStateWithErrorForTest(ctx context.Context, taskState *tasks.TaskState) error {
	return b.updateToFailureStateWithError(ctx, taskState)
}

func (b *Backend) TableExistsForTest(tableName string, tableNames []string) bool {
	return b.tableExists(tableName, tableNames)
}

func (b *Backend) CheckRequiredTablesIfExistForTest(ctx context.Context) error {
	return b.checkRequiredTablesIfExist(ctx)
}
