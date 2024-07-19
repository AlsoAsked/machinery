package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"

	"github.com/RichardKnop/machinery/v1/backends/iface"
	"github.com/RichardKnop/machinery/v1/common"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/log"
	"github.com/RichardKnop/machinery/v1/tasks"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	BatchItemsLimit  = 99
	MaxFetchAttempts = 3
)

// Backend ...
type Backend struct {
	common.Backend
	cnf    *config.Config
	client iface.DynamoDBAPI
}

// New creates a Backend instance
func New(ctx context.Context, cnf *config.Config) iface.Backend {
	backend := &Backend{Backend: common.NewBackend(cnf), cnf: cnf}

	if cnf.DynamoDB != nil && cnf.DynamoDB.Client != nil {
		// Use provided *DynamoDB client
		backend.client = cnf.DynamoDB.Client
	} else {
		// Initialize a session that the SDK will use to load credentials from the shared credentials file, ~/.aws/credentials.
		// See details on: https://docs.aws.amazon.com/sdk-for-go/v1/developer-guide/configuring-sdk.html
		// Also, env AWS_REGION is also required
		cfg, _ := awscfg.LoadDefaultConfig(context.TODO())
		backend.client = dynamodb.NewFromConfig(cfg)
	}

	backend.cnf = cnf

	// Check if needed tables exist
	err := backend.checkRequiredTablesIfExist(ctx)
	if err != nil {
		log.FATAL.Printf("Failed to prepare tables. Error: %v", err)
	}
	return backend
}

// InitGroup ...
func (b *Backend) InitGroup(ctx context.Context, groupUUID string, taskUUIDs []string) error {
	meta := tasks.GroupMeta{
		GroupUUID: groupUUID,
		TaskUUIDs: taskUUIDs,
		CreatedAt: time.Now().UTC(),
		TTL:       b.getExpirationTime(),
	}
	av, err := attributevalue.MarshalMap(meta)
	if err != nil {
		log.ERROR.Printf("Error when marshaling Dynamodb attributes. Err: %v", err)
		return err
	}
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
	}
	_, err = b.client.PutItem(ctx, input)

	if err != nil {
		log.ERROR.Printf("Got error when calling PutItem: %v; Error: %v", input, err)
		return err
	}
	return nil
}

// GroupCompleted ...
func (b *Backend) GroupCompleted(ctx context.Context, groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(ctx, groupMeta.TaskUUIDs)
	if err != nil {
		return false, err
	}
	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates ...
func (b *Backend) GroupTaskStates(ctx context.Context, groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)
	if err != nil {
		return nil, err
	}

	return b.getStates(ctx, groupMeta.TaskUUIDs)
}

// TriggerChord ...
func (b *Backend) TriggerChord(ctx context.Context, groupUUID string) (bool, error) {
	// Get the group meta data
	groupMeta, err := b.getGroupMeta(ctx, groupUUID)

	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	// If group meta is locked, wait until it's unlocked
	for groupMeta.Lock {
		groupMeta, _ = b.getGroupMeta(ctx, groupUUID)
		log.WARNING.Printf("Group [%s] locked, waiting", groupUUID)
		time.Sleep(time.Millisecond * 5)
	}

	// Acquire lock
	if err = b.lockGroupMeta(ctx, groupUUID); err != nil {
		return false, err
	}
	defer b.unlockGroupMeta(ctx, groupUUID)

	// update group meta data
	err = b.chordTriggered(ctx, groupUUID)
	if err != nil {
		return false, err
	}
	return true, err
}

// SetStatePending ...
func (b *Backend) SetStatePending(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	// taskUUID is the primary key of the table, so a new task need to be created first, instead of using dynamodb.UpdateItemInput directly
	return b.initTaskState(ctx, taskState)
}

// SetStateReceived ...
func (b *Backend) SetStateReceived(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.setTaskState(ctx, taskState)
}

// SetStateStarted ...
func (b *Backend) SetStateStarted(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.setTaskState(ctx, taskState)
}

// SetStateRetry ...
func (b *Backend) SetStateRetry(ctx context.Context, signature *tasks.Signature) error {
	taskState := tasks.NewRetryTaskState(signature)
	return b.setTaskState(ctx, taskState)
}

// SetStateSuccess ...
func (b *Backend) SetStateSuccess(ctx context.Context, signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	taskState.TTL = b.getExpirationTime()
	return b.setTaskState(ctx, taskState)
}

// SetStateFailure ...
func (b *Backend) SetStateFailure(ctx context.Context, signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	taskState.TTL = b.getExpirationTime()
	return b.updateToFailureStateWithError(ctx, taskState)
}

// GetState ...
func (b *Backend) GetState(ctx context.Context, taskUUID string) (*tasks.TaskState, error) {
	result, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
		Key: map[string]types.AttributeValue{
			"TaskUUID": &types.AttributeValueMemberS{
				Value: taskUUID,
			},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	return b.unmarshalTaskStateGetItemResult(result)
}

// getStates returns the current states for the given list of tasks.
// It uses batch fetch API. If any keys fail to fetch, it'll retry with exponential backoff until maxFetchAttempts times.
func (b *Backend) getStates(ctx context.Context, tasksToFetch []string) ([]*tasks.TaskState, error) {
	fetchedTaskStates := make([]*tasks.TaskState, 0, len(tasksToFetch))
	var unfetchedTaskIDs []string

	// try until all keys are fetched or until we run out of attempts.
	for attempt := 0; len(tasksToFetch) > 0 && attempt < MaxFetchAttempts; attempt++ {
		unfetchedTaskIDs = nil
		for _, batch := range chunkTasks(tasksToFetch, BatchItemsLimit) {
			fetched, unfetched, err := b.batchFetchTaskStates(ctx, batch)
			if err != nil {
				return nil, err
			}
			fetchedTaskStates = append(fetchedTaskStates, fetched...)
			unfetchedTaskIDs = append(unfetchedTaskIDs, unfetched...)
		}
		tasksToFetch = unfetchedTaskIDs

		// Check if there were any tasks that were not fetched. If so, retry with exponential backoff.
		if len(unfetchedTaskIDs) > 0 {
			backoffDuration := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			log.DEBUG.Printf("Unable to fetch [%d] keys on attempt [%d]. Sleeping for [%s]", len(unfetchedTaskIDs), attempt+1, backoffDuration)
			time.Sleep(backoffDuration)
		}
	}

	if len(unfetchedTaskIDs) > 0 {
		return nil, fmt.Errorf("Failed to fetch [%d] keys even after retries: [%+v]", len(unfetchedTaskIDs), unfetchedTaskIDs)
	}

	return fetchedTaskStates, nil
}

// batchFetchTaskStates returns the current states of the given tasks by fetching them all in a single batched API.
// DynamoDB's BatchGetItem() can return partial results. If there are any unfetched keys, they are returned as second
// return value so that the caller can retry those keys.
// https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb/#DynamoDB.BatchGetItem
func (b *Backend) batchFetchTaskStates(ctx context.Context, taskUUIDs []string) ([]*tasks.TaskState, []string, error) {
	tableName := b.cnf.DynamoDB.TaskStatesTable
	keys := make([]map[string]types.AttributeValue, len(taskUUIDs))
	for i, tid := range taskUUIDs {
		keys[i] = map[string]types.AttributeValue{
			"TaskUUID": &types.AttributeValueMemberS{
				Value: tid,
			},
		}
	}

	input := &dynamodb.BatchGetItemInput{
		RequestItems: map[string]types.KeysAndAttributes{
			tableName: {
				ConsistentRead: aws.Bool(true),
				Keys:           keys,
			},
		},
	}

	result, err := b.client.BatchGetItem(ctx, input)
	if err != nil {
		return nil, nil, fmt.Errorf("BatchGetItem failed. Error: [%s]", err)
	}

	fetchedKeys, ok := result.Responses[tableName]
	if !ok {
		return nil, nil, fmt.Errorf("no keys returned from the table: [%s]", tableName)
	}

	states := []*tasks.TaskState{}
	if err := attributevalue.UnmarshalListOfMaps(fetchedKeys, &states); err != nil {
		return nil, nil, fmt.Errorf("Got error when unmarshal map. Error: %v", err)
	}

	// Look for any unprocessed keys
	unfetchedKeys, err := getUnfetchedKeys(result.UnprocessedKeys[tableName])
	if err != nil {
		return nil, nil, fmt.Errorf("unable to fetch some keys: [%+v]. Error: [%s]", result.UnprocessedKeys, err)
	}

	return states, unfetchedKeys, nil
}

// PurgeState ...
func (b *Backend) PurgeState(ctx context.Context, taskUUID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"TaskUUID": &types.AttributeValueMemberS{
				Value: taskUUID,
			},
		},
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
	}
	_, err := b.client.DeleteItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

// PurgeGroupMeta ...
func (b *Backend) PurgeGroupMeta(ctx context.Context, groupUUID string) error {
	input := &dynamodb.DeleteItemInput{
		Key: map[string]types.AttributeValue{
			"GroupUUID": &types.AttributeValueMemberS{
				Value: groupUUID,
			},
		},
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
	}
	_, err := b.client.DeleteItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) getGroupMeta(ctx context.Context, groupUUID string) (*tasks.GroupMeta, error) {
	result, err := b.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(b.cnf.DynamoDB.GroupMetasTable),
		Key: map[string]types.AttributeValue{
			"GroupUUID": &types.AttributeValueMemberS{
				Value: groupUUID,
			},
		},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		log.ERROR.Printf("Error when getting group [%s]. Error: [%s]", groupUUID, err)
		return nil, err
	}
	item, err := b.unmarshalGroupMetaGetItemResult(result)
	if err != nil {
		log.ERROR.Printf("Failed to unmarshal item. Error: [%s], Result: [%+v]", err, result)
		return nil, err
	}
	return item, nil
}

func (b *Backend) lockGroupMeta(ctx context.Context, groupUUID string) error {
	err := b.updateGroupMetaLock(ctx, groupUUID, true)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) unlockGroupMeta(ctx context.Context, groupUUID string) error {
	err := b.updateGroupMetaLock(ctx, groupUUID, false)
	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) updateGroupMetaLock(ctx context.Context, groupUUID string, status bool) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{
			"#L": "Lock",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":l": &types.AttributeValueMemberBOOL{
				Value: status,
			},
		},
		Key: map[string]types.AttributeValue{
			"GroupUUID": &types.AttributeValueMemberS{
				Value: groupUUID,
			},
		},
		ReturnValues:     types.ReturnValueUpdatedNew,
		TableName:        aws.String(b.cnf.DynamoDB.GroupMetasTable),
		UpdateExpression: aws.String("SET #L = :l"),
	}

	_, err := b.client.UpdateItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) chordTriggered(ctx context.Context, groupUUID string) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{
			"#CT": "ChordTriggered",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ct": &types.AttributeValueMemberBOOL{
				Value: true,
			},
		},
		Key: map[string]types.AttributeValue{
			"GroupUUID": &types.AttributeValueMemberS{
				Value: groupUUID,
			},
		},
		ReturnValues:     types.ReturnValueUpdatedNew,
		TableName:        aws.String(b.cnf.DynamoDB.GroupMetasTable),
		UpdateExpression: aws.String("SET #CT = :ct"),
	}

	_, err := b.client.UpdateItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) setTaskState(ctx context.Context, taskState *tasks.TaskState) error {
	expAttributeNames := map[string]string{
		"#S": "State",
	}
	expAttributeValues := map[string]types.AttributeValue{
		":s": &types.AttributeValueMemberS{
			Value: taskState.State,
		},
	}
	keyAttributeValues := map[string]types.AttributeValue{
		"TaskUUID": &types.AttributeValueMemberS{
			Value: taskState.TaskUUID,
		},
	}
	exp := "SET #S = :s"
	if !taskState.CreatedAt.IsZero() {
		expAttributeNames["#C"] = "CreatedAt"
		expAttributeValues[":c"] = &types.AttributeValueMemberS{
			Value: taskState.CreatedAt.String(),
		}
		exp += ", #C = :c"
	}
	if taskState.TTL > 0 {
		expAttributeNames["#T"] = "TTL"
		expAttributeValues[":t"] = &types.AttributeValueMemberN{
			Value: fmt.Sprintf("%d", taskState.TTL),
		}
		exp += ", #T = :t"
	}
	if taskState.Results != nil && len(taskState.Results) != 0 {
		expAttributeNames["#R"] = "Results"
		var results []types.AttributeValue
		for _, r := range taskState.Results {
			avMap := map[string]types.AttributeValue{
				"Type": &types.AttributeValueMemberS{
					Value: r.Type,
				},
				"Value": &types.AttributeValueMemberS{
					Value: fmt.Sprintf("%v", r.Value),
				},
			}
			rs := &types.AttributeValueMemberM{
				Value: avMap,
			}
			results = append(results, rs)
		}
		expAttributeValues[":r"] = &types.AttributeValueMemberL{
			Value: results,
		}
		exp += ", #R = :r"
	}
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames:  expAttributeNames,
		ExpressionAttributeValues: expAttributeValues,
		Key:                       keyAttributeValues,
		ReturnValues:              types.ReturnValueUpdatedNew,
		TableName:                 aws.String(b.cnf.DynamoDB.TaskStatesTable),
		UpdateExpression:          aws.String(exp),
	}

	_, err := b.client.UpdateItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) initTaskState(ctx context.Context, taskState *tasks.TaskState) error {
	av, err := attributevalue.MarshalMap(taskState)
	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(b.cnf.DynamoDB.TaskStatesTable),
	}
	if err != nil {
		return err
	}
	_, err = b.client.PutItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) updateToFailureStateWithError(ctx context.Context, taskState *tasks.TaskState) error {
	input := &dynamodb.UpdateItemInput{
		ExpressionAttributeNames: map[string]string{
			"#S": "State",
			"#E": "Error",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":s": &types.AttributeValueMemberS{
				Value: taskState.State,
			},
			":e": &types.AttributeValueMemberS{
				Value: taskState.Error,
			},
		},
		Key: map[string]types.AttributeValue{
			"TaskUUID": &types.AttributeValueMemberS{
				Value: taskState.TaskUUID,
			},
		},
		ReturnValues:     types.ReturnValueUpdatedNew,
		TableName:        aws.String(b.cnf.DynamoDB.TaskStatesTable),
		UpdateExpression: aws.String("SET #S = :s, #E = :e"),
	}

	if taskState.TTL > 0 {
		input.ExpressionAttributeNames["#T"] = "TTL"
		input.ExpressionAttributeValues[":t"] = &types.AttributeValueMemberN{
			Value: fmt.Sprintf("%d", taskState.TTL),
		}
		input.UpdateExpression = aws.String(aws.ToString(input.UpdateExpression) + ", #T = :t")
	}

	_, err := b.client.UpdateItem(ctx, input)

	if err != nil {
		return err
	}
	return nil
}

func (b *Backend) unmarshalGroupMetaGetItemResult(result *dynamodb.GetItemOutput) (*tasks.GroupMeta, error) {
	if result == nil {
		err := errors.New("task state is nil")
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	item := tasks.GroupMeta{}
	err := attributevalue.UnmarshalMap(result.Item, &item)
	if err != nil {
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	return &item, err
}

func (b *Backend) unmarshalTaskStateGetItemResult(result *dynamodb.GetItemOutput) (*tasks.TaskState, error) {
	if result == nil {
		err := errors.New("task state is nil")
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	state := tasks.TaskState{}
	err := attributevalue.UnmarshalMap(result.Item, &state)
	if err != nil {
		log.ERROR.Printf("Got error when unmarshal map. Error: %v", err)
		return nil, err
	}
	return &state, nil
}

func (b *Backend) checkRequiredTablesIfExist(ctx context.Context) error {
	var (
		taskTableName  = b.cnf.DynamoDB.TaskStatesTable
		groupTableName = b.cnf.DynamoDB.GroupMetasTable
	)
	result, err := b.client.ListTables(ctx, &dynamodb.ListTablesInput{})
	if err != nil {
		return err
	}
	if !b.tableExists(taskTableName, result.TableNames) {
		return errors.New("task table doesn't exist")
	}
	if !b.tableExists(groupTableName, result.TableNames) {
		return errors.New("group table doesn't exist")
	}
	return nil
}

func (b *Backend) tableExists(tableName string, tableNames []string) bool {
	for _, t := range tableNames {
		if tableName == t {
			return true
		}
	}
	return false
}

func (b *Backend) getExpirationTime() int64 {
	expiresIn := b.GetConfig().ResultsExpireIn
	if expiresIn == 0 {
		// expire results after 1 hour by default
		expiresIn = config.DefaultResultsExpireIn
	}
	return time.Now().Add(time.Second * time.Duration(expiresIn)).Unix()
}

// getUnfetchedKeys returns keys that were not fetched in a batch request.
func getUnfetchedKeys(unprocessed types.KeysAndAttributes) ([]string, error) {
	states := []*tasks.TaskState{}
	var taskIDs []string
	if err := attributevalue.UnmarshalListOfMaps(unprocessed.Keys, &states); err != nil {
		return nil, fmt.Errorf("Got error when unmarshal map. Error: %v", err)
	}
	for _, s := range states {
		taskIDs = append(taskIDs, s.TaskUUID)
	}
	return taskIDs, nil
}

// chunkTasks chunks the list of strings into multiple smaller lists of specified size.
func chunkTasks(array []string, chunkSize int) [][]string {
	var result [][]string
	for len(array) > 0 {
		sz := min(len(array), chunkSize)
		chunk := array[:sz]
		array = array[sz:]
		result = append(result, chunk)
	}
	return result
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
