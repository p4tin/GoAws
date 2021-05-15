package conf

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/ghodss/yaml"
	"github.com/p4tin/goaws/app"
	"github.com/p4tin/goaws/app/common"
)

var envs map[string]app.Environment

func LoadYamlConfig(filename string, env string) []string {
	ports := []string{"4100"}

	if filename == "" {
		filename, _ = filepath.Abs("./conf/goaws.yaml")
	}
	log.Warnf("Loading config file: %s", filename)
	yamlFile, err := ioutil.ReadFile(filename)
	if err != nil {
		return ports
	}

	err = yaml.Unmarshal(yamlFile, &envs)
	if err != nil {
		log.Errorf("err: %v\n", err)
		return ports
	}
	if env == "" {
		env = "Local"
	}

	if envs[env].Region == "" {
		app.CurrentEnvironment.Region = "local"
	}

	app.CurrentEnvironment = envs[env]

	if envs[env].Port != "" {
		ports = []string{envs[env].Port}
	} else if envs[env].SqsPort != "" && envs[env].SnsPort != "" {
		ports = []string{envs[env].SqsPort, envs[env].SnsPort}
		app.CurrentEnvironment.Port = envs[env].SqsPort
	}

	common.LogMessages = false
	common.LogFile = "./goaws_messages.log"

	if envs[env].LogToFile == true {
		common.LogMessages = true
		if envs[env].LogFile != "" {
			common.LogFile = envs[env].LogFile
		}
	}

	if app.CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout == 0 {
		app.CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout = 30
	}

	if app.CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize == 0 {
		app.CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize = 262144 // 256K
	}

	if app.CurrentEnvironment.AccountID == "" {
		app.CurrentEnvironment.AccountID = "queue"
	}

	app.SyncQueues.Lock()
	app.SyncTopics.Lock()
	for _, queue := range envs[env].Queues {
		queueUrl := "http://%s/" + app.CurrentEnvironment.AccountID + "/" + queue.Name
		if app.CurrentEnvironment.Region != "" {
			queueUrl = "http://" + app.CurrentEnvironment.Region + ".%s/" + app.CurrentEnvironment.AccountID + "/" + queue.Name
		}
		queueArn := "arn:aws:sqs:" + app.CurrentEnvironment.Region + ":" + app.CurrentEnvironment.AccountID + ":" + queue.Name

		if queue.ReceiveMessageWaitTimeSeconds == 0 {
			queue.ReceiveMessageWaitTimeSeconds = app.CurrentEnvironment.QueueAttributeDefaults.ReceiveMessageWaitTimeSeconds
		}
		if queue.MaximumMessageSize == 0 {
			queue.MaximumMessageSize = app.CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize
		}

		app.SyncQueues.Queues[queue.Name] = &app.Queue{
			Name:                queue.Name,
			TimeoutSecs:         app.CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout,
			Arn:                 queueArn,
			URL:                 queueUrl,
			ReceiveWaitTimeSecs: queue.ReceiveMessageWaitTimeSeconds,
			MaximumMessageSize:  queue.MaximumMessageSize,
			IsFIFO:              app.HasFIFOQueueName(queue.Name),
			EnableDuplicates:    app.CurrentEnvironment.EnableDuplicates,
			Duplicates:          make(map[string]time.Time),
		}
	}

	// loop one more time to create queue's RedrivePolicy and assign deadletter queues in case dead letter queue is defined first in the config
	for _, queue := range envs[env].Queues {
		q := app.SyncQueues.Queues[queue.Name]
		if queue.RedrivePolicy != "" {
			err := setQueueRedrivePolicy(app.SyncQueues.Queues, q, queue.RedrivePolicy)
			if err != nil {
				log.Errorf("err: %s", err)
				return ports
			}
		}

	}

	for _, topic := range envs[env].Topics {
		topicArn := "arn:aws:sns:" + app.CurrentEnvironment.Region + ":" + app.CurrentEnvironment.AccountID + ":" + topic.Name

		newTopic := &app.Topic{Name: topic.Name, Arn: topicArn}
		newTopic.Subscriptions = make([]*app.Subscription, 0, 0)

		for _, subs := range topic.Subscriptions {
			var newSub *app.Subscription
			if strings.Contains(subs.Protocol, "http") {
				newSub = createHttpSubscription(subs)
			} else {
				//Queue does not exist yet, create it.
				newSub = createSqsSubscription(subs, topicArn)
			}
			if subs.FilterPolicy != "" {
				filterPolicy := &app.FilterPolicy{}
				err = json.Unmarshal([]byte(subs.FilterPolicy), filterPolicy)
				if err != nil {
					log.Errorf("err: %s", err)
					return ports
				}
				newSub.FilterPolicy = filterPolicy
			}

			newTopic.Subscriptions = append(newTopic.Subscriptions, newSub)
		}
		app.SyncTopics.Topics[topic.Name] = newTopic
	}

	app.SyncQueues.Unlock()
	app.SyncTopics.Unlock()

	return ports
}

func createHttpSubscription(configSubscription app.EnvSubsciption) *app.Subscription {
	newSub := &app.Subscription{EndPoint: configSubscription.EndPoint, Protocol: configSubscription.Protocol, TopicArn: configSubscription.TopicArn, Raw: configSubscription.Raw}
	subArn, _ := common.NewUUID()
	subArn = configSubscription.TopicArn + ":" + subArn
	newSub.SubscriptionArn = subArn
	return newSub
}

func createSqsSubscription(configSubscription app.EnvSubsciption, topicArn string) *app.Subscription {
	if _, ok := app.SyncQueues.Queues[configSubscription.QueueName]; !ok {
		queueUrl := "http://%s/" + app.CurrentEnvironment.AccountID + "/" + configSubscription.QueueName
		if app.CurrentEnvironment.Region != "" {
			queueUrl = "http://" + app.CurrentEnvironment.Region + ".%s/" + app.CurrentEnvironment.AccountID + "/" + configSubscription.QueueName
		}
		queueArn := "arn:aws:sqs:" + app.CurrentEnvironment.Region + ":" + app.CurrentEnvironment.AccountID + ":" + configSubscription.QueueName
		app.SyncQueues.Queues[configSubscription.QueueName] = &app.Queue{
			Name:                configSubscription.QueueName,
			TimeoutSecs:         app.CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout,
			Arn:                 queueArn,
			URL:                 queueUrl,
			ReceiveWaitTimeSecs: app.CurrentEnvironment.QueueAttributeDefaults.ReceiveMessageWaitTimeSeconds,
			MaximumMessageSize:  app.CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize,
			IsFIFO:              app.HasFIFOQueueName(configSubscription.QueueName),
			EnableDuplicates:    app.CurrentEnvironment.EnableDuplicates,
			Duplicates:          make(map[string]time.Time),
		}
	}
	qArn := app.SyncQueues.Queues[configSubscription.QueueName].Arn
	newSub := &app.Subscription{EndPoint: qArn, Protocol: "sqs", TopicArn: topicArn, Raw: configSubscription.Raw}
	subArn, _ := common.NewUUID()
	subArn = topicArn + ":" + subArn
	newSub.SubscriptionArn = subArn
	return newSub
}

func setQueueRedrivePolicy(queues map[string]*app.Queue, q *app.Queue, strRedrivePolicy string) error {
	// support both int and string maxReceiveCount (Amazon clients use string)
	redrivePolicy1 := struct {
		MaxReceiveCount     int    `json:"maxReceiveCount"`
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}{}
	redrivePolicy2 := struct {
		MaxReceiveCount     string `json:"maxReceiveCount"`
		DeadLetterTargetArn string `json:"deadLetterTargetArn"`
	}{}
	err1 := json.Unmarshal([]byte(strRedrivePolicy), &redrivePolicy1)
	err2 := json.Unmarshal([]byte(strRedrivePolicy), &redrivePolicy2)
	maxReceiveCount := redrivePolicy1.MaxReceiveCount
	deadLetterQueueArn := redrivePolicy1.DeadLetterTargetArn
	if err1 != nil && err2 != nil {
		return fmt.Errorf("invalid json for queue redrive policy ")
	} else if err1 != nil {
		maxReceiveCount, _ = strconv.Atoi(redrivePolicy2.MaxReceiveCount)
		deadLetterQueueArn = redrivePolicy2.DeadLetterTargetArn
	}

	if (deadLetterQueueArn != "" && maxReceiveCount == 0) ||
		(deadLetterQueueArn == "" && maxReceiveCount != 0) {
		return fmt.Errorf("invalid redrive policy values")
	}
	dlt := strings.Split(deadLetterQueueArn, ":")
	deadLetterQueueName := dlt[len(dlt)-1]
	deadLetterQueue, ok := queues[deadLetterQueueName]
	if !ok {
		return fmt.Errorf("deadletter queue not found")
	}
	q.DeadLetterQueue = deadLetterQueue
	q.MaxReceiveCount = maxReceiveCount

	return nil
}
