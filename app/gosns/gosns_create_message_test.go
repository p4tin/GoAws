package gosns

import (
	"encoding/json"
	"testing"

	"github.com/p4tin/goaws/app"
)

const (
	messageKey            = "Message"
	subjectKey            = "Subject"
	messageStructureJSON  = "json"
	messageStructureEmpty = ""
)

// When simple message string is passed,
// it must be used for all subscribers (no matter the protocol)
func TestCreateMessageBody_NonJson(t *testing.T) {
	message := "message text"
	subject := "subject"
	host := "localhost:4100"
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}

	snsMessage, err := CreateMessageBody(host, subs, message, subject, messageStructureEmpty, make(map[string]app.MessageAttributeValue))
	if err != nil {
		t.Fatalf(`error creating SNS message: %s`, err)
	}

	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	if err != nil {
		t.Fatalf(`error unmarshalling SNS message "%s": %s`, snsMessage, err)
	}

	receivedMessage, ok := unmarshalled[messageKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	}

	if receivedMessage != message {
		t.Errorf(`expected message "%s" but received "%s"`, message, receivedMessage)
	}

	receivedSubject, ok := unmarshalled[subjectKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	}

	if receivedSubject != subject {
		t.Errorf(`expected subject "%s" but received "%s"`, subject, receivedSubject)
	}
}

// When no protocol specific message is passed,
// default message must be forwarded
func TestCreateMessageBody_OnlyDefaultValueInJson(t *testing.T) {
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	message := `{"default": "default message text", "http": "HTTP message text"}`
	subject := "subject"
	host := "localhost:4100"

	snsMessage, err := CreateMessageBody(host, subs, message, subject, messageStructureJSON, nil)
	if err != nil {
		t.Fatalf(`error creating SNS message: %s`, err)
	}

	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	if err != nil {
		t.Fatalf(`error unmarshalling SNS message "%s": %s`, snsMessage, err)
	}

	receivedMessage, ok := unmarshalled[messageKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	}

	expected := "default message text"
	if receivedMessage != expected {
		t.Errorf(`expected message "%s" but received "%s"`, expected, receivedMessage)
	}

	receivedSubject, ok := unmarshalled[subjectKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	}

	if receivedSubject != subject {
		t.Errorf(`expected subject "%s" but received "%s"`, subject, receivedSubject)
	}
}

// When only protocol specific message is passed,
// error must be returned
func TestCreateMessageBody_OnlySqsValueInJson(t *testing.T) {
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	message := `{"sqs": "message text"}`
	subject := "subject"
	host := "localhost:4100"

	snsMessage, err := CreateMessageBody(host, subs, message, subject, messageStructureJSON, nil)
	if err == nil {
		t.Fatalf(`error expected but instead SNS message was returned: %s`, snsMessage)
	}
}

// when both default and protocol specific messages are passed,
// protocol specific message must be used
func TestCreateMessageBody_BothDefaultAndSqsValuesInJson(t *testing.T) {
	subs := &app.Subscription{
		Protocol:        "sqs",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	message := `{"default": "default message text", "sqs": "sqs message text"}`
	subject := "subject"
	host := "localhost:4100"

	snsMessage, err := CreateMessageBody(host, subs, message, subject, messageStructureJSON, nil)
	if err != nil {
		t.Fatalf(`error creating SNS message: %s`, err)
	}

	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	if err != nil {
		t.Fatalf(`error unmarshalling SNS message "%s": %s`, snsMessage, err)
	}

	receivedMessage, ok := unmarshalled[messageKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	}

	expected := "sqs message text"
	if receivedMessage != expected {
		t.Errorf(`expected message "%s" but received "%s"`, expected, receivedMessage)
	}

	receivedSubject, ok := unmarshalled[subjectKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	}

	if receivedSubject != subject {
		t.Errorf(`expected subject "%s" but received "%s"`, subject, receivedSubject)
	}
}

// When simple message string is passed,
// it must be used as is (even if it contains JSON)
func TestCreateMessageBody_NonJsonContainingJson(t *testing.T) {
	subs := &app.Subscription{
		Protocol:        "sns",
		TopicArn:        "topic-arn",
		SubscriptionArn: "subs-arn",
		Raw:             false,
	}
	message := `{"default": "default message text", "sqs": "sqs message text"}`
	subject := "subject"
	host := "localhost:4100"

	snsMessage, err := CreateMessageBody(host, subs, message, subject, "", nil)
	if err != nil {
		t.Fatalf(`error creating SNS message: %s`, err)
	}

	var unmarshalled map[string]interface{}
	err = json.Unmarshal(snsMessage, &unmarshalled)
	if err != nil {
		t.Fatalf(`error unmarshalling SNS message "%s": %s`, snsMessage, err)
	}

	receivedMessage, ok := unmarshalled[messageKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, messageKey)
	}

	expected := `{"default": "default message text", "sqs": "sqs message text"}`
	if receivedMessage != expected {
		t.Errorf(`expected message "%s" but received "%s"`, expected, receivedMessage)
	}

	receivedSubject, ok := unmarshalled[subjectKey]
	if !ok {
		t.Fatalf(`SNS message "%s" does not contain key "%s"`, snsMessage, subjectKey)
	}

	if receivedSubject != subject {
		t.Errorf(`expected subject "%s" but received "%s"`, subject, receivedSubject)
	}
}
