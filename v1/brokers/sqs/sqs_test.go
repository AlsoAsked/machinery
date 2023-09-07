package sqs_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/stretchr/testify/assert"

	"github.com/RichardKnop/machinery/v1"
	"github.com/RichardKnop/machinery/v1/brokers/sqs"
	"github.com/RichardKnop/machinery/v1/config"
	"github.com/RichardKnop/machinery/v1/retry"

	awssqs "github.com/aws/aws-sdk-go/service/sqs"
)

var (
	cnf                  *config.Config
	receiveMessageOutput *awssqs.ReceiveMessageOutput
)

func init() {
	cnf = sqs.NewTestConfig()
	receiveMessageOutput = sqs.ReceiveMessageOutput
}

func TestNewAWSSQSBroker(t *testing.T) {
	t.Parallel()

	broker := sqs.NewTestBroker()

	assert.IsType(t, broker, sqs.New(cnf))
}

func TestPrivateFunc_continueReceivingMessages(t *testing.T) {

	broker := sqs.NewTestBroker()
	errorBroker := sqs.NewTestErrorBroker()

	qURL := broker.DefaultQueueURLForTest()
	deliveries := make(chan *awssqs.ReceiveMessageOutput)
	firstStep := make(chan int)
	nextStep := make(chan int)
	go func() {
		stopReceivingChan := broker.GetStopReceivingChanForTest()
		firstStep <- 1
		stopReceivingChan <- 1
	}()

	var (
		whetherContinue bool
		err             error
	)
	<-firstStep
	// Test the case that a signal was received from stopReceivingChan
	go func() {
		whetherContinue, err = broker.ContinueReceivingMessagesForTest(qURL, deliveries)
		nextStep <- 1
	}()
	<-nextStep
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	// Test the default condition
	whetherContinue, err = broker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	// Test the error
	whetherContinue, err = errorBroker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.NotNil(t, err)

	// Test when there is no message
	outputCopy := *receiveMessageOutput
	receiveMessageOutput.Messages = []*awssqs.Message{}
	whetherContinue, err = broker.ContinueReceivingMessagesForTest(qURL, deliveries)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)
	// recover original value
	*receiveMessageOutput = outputCopy
}

func TestPrivateFunc_consume(t *testing.T) {

	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	pool := make(chan struct{})
	wk := server1.NewWorker("sms_worker", 0)
	deliveries := make(chan *sqs.ReceivedMessages)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	receivedMsg := sqs.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()

	broker := sqs.NewTestBroker()

	// an infinite loop will be executed only when there is no error
	err = broker.ConsumeForTest(deliveries, 0, wk, pool)
	assert.NotNil(t, err)
}

func TestPrivateFunc_consumeOne(t *testing.T) {

	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	wk := server1.NewWorker("sms_worker", 0)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	receivedMsg := sqs.ReceivedMessages{Delivery: &outputCopy}
	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)

	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)

	outputCopy.Messages = []*awssqs.Message{
		{
			Body: aws.String("foo message"),
		},
	}
	receivedMsg = sqs.ReceivedMessages{Delivery: &outputCopy}
	err = testAWSSQSBroker.ConsumeOneForTest(&receivedMsg, wk)
	assert.NotNil(t, err)
}

func TestPrivateFunc_initializePool(t *testing.T) {

	broker := sqs.NewTestBroker()

	concurrency := 9
	pool := make(chan struct{}, concurrency)
	broker.InitializePoolForTest(pool, concurrency)
	assert.Len(t, pool, concurrency)
}

func TestPrivateFunc_startConsuming(t *testing.T) {

	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	broker := sqs.NewTestBroker()

	retryFunc := broker.GetRetryFuncForTest()
	stopChan := broker.GetStopChanForTest()
	retryStopChan := broker.GetRetryStopChanForTest()
	assert.Nil(t, retryFunc)

	broker.StartConsumingForTest("fooTag", 1, wk)
	assert.IsType(t, retryFunc, retry.Closure())
	assert.Equal(t, len(stopChan), 0)
	assert.Equal(t, len(retryStopChan), 0)
}

func TestPrivateFuncDefaultQueueURL(t *testing.T) {

	broker := sqs.NewTestBroker()

	qURL := broker.DefaultQueueURLForTest()

	assert.EqualValues(t, *qURL, "https://sqs.foo.amazonaws.com.cn/test_queue")
}

func TestPrivateFunc_stopReceiving(t *testing.T) {

	broker := sqs.NewTestBroker()

	go broker.StopReceivingForTest()

	stopReceivingChan := broker.GetStopReceivingChanForTest()
	assert.NotNil(t, <-stopReceivingChan)
}

func TestPrivateFunc_receiveMessage(t *testing.T) {

	broker := sqs.NewTestBroker()

	qURL := broker.DefaultQueueURLForTest()
	output, err := broker.ReceiveMessageForTest(qURL)
	assert.Nil(t, err)
	assert.Equal(t, receiveMessageOutput, output)
}

func TestPrivateFunc_consumeDeliveries(t *testing.T) {

	concurrency := 0
	pool := make(chan struct{}, concurrency)
	errorsChan := make(chan error)
	deliveries := make(chan *sqs.ReceivedMessages)
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	wk := server1.NewWorker("sms_worker", 0)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	receivedMsg := sqs.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()
	whetherContinue, err := testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.True(t, whetherContinue)
	assert.Nil(t, err)

	go func() { errorsChan <- errors.New("foo error") }()
	whetherContinue, err = broker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.NotNil(t, err)

	go func() { broker.GetStopChanForTest() <- 1 }()
	whetherContinue, err = broker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	assert.False(t, whetherContinue)
	assert.Nil(t, err)

	outputCopy = *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	receivedMsg = sqs.ReceivedMessages{Delivery: &outputCopy}
	go func() { deliveries <- &receivedMsg }()
	whetherContinue, err = testAWSSQSBroker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	e := <-errorsChan
	assert.True(t, whetherContinue)
	assert.NotNil(t, e)
	assert.Nil(t, err)

	// using a wait group and a channel to fix the racing problem
	outputCopy = *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{}
	receivedMsg = sqs.ReceivedMessages{Delivery: &outputCopy}
	var wg sync.WaitGroup
	wg.Add(1)
	nextStep := make(chan bool, 1)
	go func() {
		defer wg.Done()
		// nextStep <- true runs after defer wg.Done(), to make sure the next go routine runs after this go routine
		nextStep <- true
		deliveries <- &receivedMsg
	}()
	if <-nextStep {
		// <-pool will block the routine in the following steps, so pool <- struct{}{} will be executed for sure
		go func() { wg.Wait(); pool <- struct{}{} }()
	}
	whetherContinue, err = broker.ConsumeDeliveriesForTest(deliveries, concurrency, wk, pool, errorsChan)
	// the pool shouldn't be consumed
	p := <-pool
	assert.True(t, whetherContinue)
	assert.NotNil(t, p)
	assert.Nil(t, err)
}

func TestPrivateFunc_deleteOne(t *testing.T) {
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{
		{
			Body:          aws.String("foo message"),
			ReceiptHandle: aws.String("receipthandle"),
		},
	}
	receivedMsg := sqs.ReceivedMessages{Delivery: &outputCopy}
	err := testAWSSQSBroker.DeleteOneForTest(&receivedMsg)
	assert.Nil(t, err)

	err = errAWSSQSBroker.DeleteOneForTest(&receivedMsg)
	assert.NotNil(t, err)
}

func Test_CustomQueueName(t *testing.T) {

	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	broker := sqs.NewTestBroker()

	wk := server1.NewWorker("test-worker", 0)
	qURL := broker.GetQueueURLForTest(wk)
	assert.Equal(t, qURL, broker.DefaultQueueURLForTest(), "")

	wk2 := server1.NewCustomQueueWorker("test-worker", 0, "my-custom-queue")
	qURL2 := broker.GetQueueURLForTest(wk2)
	assert.Equal(t, qURL2, broker.GetCustomQueueURL("my-custom-queue"), "")
}

func TestPrivateFunc_consumeWithConcurrency(t *testing.T) {

	msg := `{
        "UUID": "uuid-dummy-task",
        "Name": "test-task",
        "RoutingKey": "dummy-routing"
	}
	`

	testResp := "47f8b355-5115-4b45-b33a-439016400411"
	output := make(chan string) // The output channel

	cnf.ResultBackend = "eager"
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}
	err = server1.RegisterTask("test-task", func(ctx context.Context) error {
		output <- testResp

		return nil
	})

	broker := sqs.NewTestBroker()

	broker.SetRegisteredTaskNames([]string{"test-task"})
	assert.NoError(t, err)
	pool := make(chan struct{}, 1)
	pool <- struct{}{}
	wk := server1.NewWorker("sms_worker", 1)
	deliveries := make(chan *sqs.ReceivedMessages)
	outputCopy := *receiveMessageOutput
	outputCopy.Messages = []*awssqs.Message{
		{
			MessageId: aws.String("test-sqs-msg1"),
			Body:      aws.String(msg),
		},
	}
	receivedMsg := sqs.ReceivedMessages{Delivery: &outputCopy}
	go func() {
		deliveries <- &receivedMsg

	}()

	go func() {
		err = broker.ConsumeForTest(deliveries, 1, wk, pool)
	}()

	select {
	case resp := <-output:
		assert.Equal(t, testResp, resp)

	case <-time.After(10 * time.Second):
		// call timed out
		t.Fatal("task not processed in 10 seconds")
	}
}

type roundRobinQueues struct {
	queues       []string
	currentIndex int
}

func NewRoundRobinQueues(queues []string) *roundRobinQueues {
	return &roundRobinQueues{
		queues:       queues,
		currentIndex: -1,
	}
}

func (r *roundRobinQueues) Peek() string {
	return r.queues[r.currentIndex]
}

func (r *roundRobinQueues) Next() string {
	r.currentIndex += 1
	if r.currentIndex >= len(r.queues) {
		r.currentIndex = 0
	}

	q := r.queues[r.currentIndex]
	return q
}

func TestPrivateFunc_consumeWithRoundRobinQueues(t *testing.T) {
	server1, err := machinery.NewServer(cnf)
	if err != nil {
		t.Fatal(err)
	}

	w := server1.NewWorker("test-worker", 0)

	// Assigning a getQueueHandler to `Next` method of roundRobinQueues
	rr := NewRoundRobinQueues([]string{"custom-queue-0", "custom-queue-1", "custom-queue-2", "custom-queue-3"})
	w.SetGetQueueHandler(rr.Next)

	for i := 0; i < 5; i++ {
		// the queue url of the broker should match the current queue url of roundRobin
		// and thus queues are being utilized in round-robin fashion
		qURL := testAWSSQSBroker.GetQueueURLForTest(w)
		assert.Equal(t, qURL, testAWSSQSBroker.GetCustomQueueURL(rr.Peek()))
	}
}
