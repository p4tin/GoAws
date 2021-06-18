package app

import (
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type SqsErrorType struct {
	HttpError int
	Type      string
	Code      string
	Message   string
}

func (s *SqsErrorType) Error() string {
	return s.Type
}

var SqsErrors map[string]SqsErrorType

type Message struct {
	MessageBody            []byte
	Uuid                   string
	MD5OfMessageAttributes string
	MD5OfMessageBody       string
	ReceiptHandle          string
	ReceiptTime            time.Time
	VisibilityTimeout      time.Time
	NumberOfReceives       int
	Retry                  int
	MessageAttributes      map[string]MessageAttributeValue
	GroupID                string
	DeduplicationID        string
	SentTime               time.Time
	DelaySecs              int
}

func (m *Message) IsReadyForReceipt() bool {
	randomLatency, err := getRandomLatency()
	if err != nil {
		log.Error(err)
		return true
	}
	showAt := m.SentTime.Add(randomLatency).Add(time.Duration(m.DelaySecs) * time.Second)
	return showAt.Before(time.Now())
}

func getRandomLatency() (time.Duration, error) {
	min := CurrentEnvironment.RandomLatency.Min
	max := CurrentEnvironment.RandomLatency.Max
	if min == 0 && max == 0 {
		return time.Duration(0), nil
	}
	var randomLatencyValue int
	if max == min {
		randomLatencyValue = max
	} else {
		randomLatencyValue = rand.Intn(max-min) + min
	}
	randomDuration, err := time.ParseDuration(fmt.Sprintf("%dms", randomLatencyValue))
	if err != nil {
		return time.Duration(0), errors.New(fmt.Sprintf("Error parsing random latency value: %dms", randomLatencyValue))
	}
	return randomDuration, nil
}

type MessageAttributeValue struct {
	Name     string
	DataType string
	Value    string
	ValueKey string
}

type Queue struct {
	Name                string
	URL                 string
	Arn                 string
	TimeoutSecs         int
	ReceiveWaitTimeSecs int
	DelaySecs           int
	MaximumMessageSize  int
	Messages            []Message
	DeadLetterQueue     *Queue
	MaxReceiveCount     int
	IsFIFO              bool
	FIFOMessages        map[string]int
	FIFOSequenceNumbers map[string]int
	EnableDuplicates    bool
	Duplicates          map[string]time.Time
}

var SyncQueues = struct {
	sync.RWMutex
	Queues map[string]*Queue
}{Queues: make(map[string]*Queue)}

var DeduplicationPeriod = 5 * time.Minute

func HasFIFOQueueName(queueName string) bool {
	return strings.HasSuffix(queueName, ".fifo")
}

func (q *Queue) NextSequenceNumber(groupId string) string {
	if _, ok := q.FIFOSequenceNumbers[groupId]; !ok {
		q.FIFOSequenceNumbers = map[string]int{
			groupId: 0,
		}
	}

	q.FIFOSequenceNumbers[groupId]++
	return strconv.Itoa(q.FIFOSequenceNumbers[groupId])
}

func (q *Queue) IsLocked(groupId string) bool {
	_, ok := q.FIFOMessages[groupId]
	return ok
}

func (q *Queue) LockGroup(groupId string) {
	if _, ok := q.FIFOMessages[groupId]; !ok {
		q.FIFOMessages = map[string]int{
			groupId: 0,
		}
	}
}

func (q *Queue) UnlockGroup(groupId string) {
	if _, ok := q.FIFOMessages[groupId]; ok {
		delete(q.FIFOMessages, groupId)
	}
}

func (q *Queue) IsDuplicate(deduplicationId string) bool {
	if !q.EnableDuplicates || !q.IsFIFO || deduplicationId == "" {
		return false
	}

	_, ok := q.Duplicates[deduplicationId]

	return ok
}

func (q *Queue) InitDuplicatation(deduplicationId string) {
	if !q.EnableDuplicates || !q.IsFIFO || deduplicationId == "" {
		return
	}

	if _, ok := q.Duplicates[deduplicationId]; !ok {
		q.Duplicates[deduplicationId] = time.Now()
	}
}
