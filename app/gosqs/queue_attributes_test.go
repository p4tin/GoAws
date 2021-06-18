package gosqs

import (
	"net/url"
	"reflect"
	"testing"

	"github.com/p4tin/goaws/app"
)

func TestApplyQueueAttributes(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		deadLetterQueue := &app.Queue{Name: "failed-messages"}
		app.SyncQueues.Lock()
		app.SyncQueues.Queues["failed-messages"] = deadLetterQueue
		app.SyncQueues.Unlock()
		q := &app.Queue{TimeoutSecs: 30}
		u := url.Values{}
		u.Add("Attribute.1.Name", "DelaySeconds")
		u.Add("Attribute.1.Value", "25")
		u.Add("Attribute.2.Name", "VisibilityTimeout")
		u.Add("Attribute.2.Value", "60")
		u.Add("Attribute.3.Name", "Policy")
		u.Add("Attribute.4.Name", "RedrivePolicy")
		u.Add("Attribute.4.Value", `{"maxReceiveCount": "4", "deadLetterTargetArn":"arn:aws:sqs::000000000000:failed-messages"}`)
		u.Add("Attribute.5.Name", "ReceiveMessageWaitTimeSeconds")
		u.Add("Attribute.5.Value", "20")
		if err := validateAndSetQueueAttributes(q, u); err != nil {
			t.Fatalf("expected nil, got %s", err)
		}
		expected := &app.Queue{
			TimeoutSecs:         60,
			ReceiveWaitTimeSecs: 20,
			DelaySecs:           25,
			MaxReceiveCount:     4,
			DeadLetterQueue:     deadLetterQueue,
		}
		if ok := reflect.DeepEqual(q, expected); !ok {
			t.Fatalf("expected %+v, got %+v", expected, q)
		}
	})
	t.Run("missing_deadletter_arn", func(t *testing.T) {
		q := &app.Queue{TimeoutSecs: 30}
		u := url.Values{}
		u.Add("Attribute.1.Name", "RedrivePolicy")
		u.Add("Attribute.1.Value", `{"maxReceiveCount": "4"}`)
		err := validateAndSetQueueAttributes(q, u)
		if err != ErrInvalidParameterValue {
			t.Fatalf("expected %s, got %s", ErrInvalidParameterValue, err)
		}
	})
	t.Run("invalid_redrive_policy", func(t *testing.T) {
		q := &app.Queue{TimeoutSecs: 30}
		u := url.Values{}
		u.Add("Attribute.1.Name", "RedrivePolicy")
		u.Add("Attribute.1.Value", `{invalidinput}`)
		err := validateAndSetQueueAttributes(q, u)
		if err != ErrInvalidAttributeValue {
			t.Fatalf("expected %s, got %s", ErrInvalidAttributeValue, err)
		}
	})
}

func TestExtractQueueAttributes(t *testing.T) {
	u := url.Values{}
	u.Add("Attribute.1.Name", "DelaySeconds")
	u.Add("Attribute.1.Value", "20")
	u.Add("Attribute.2.Name", "VisibilityTimeout")
	u.Add("Attribute.2.Value", "30")
	u.Add("Attribute.3.Name", "Policy")
	attr := extractQueueAttributes(u)
	expected := map[string]string{
		"DelaySeconds":      "20",
		"VisibilityTimeout": "30",
	}
	if ok := reflect.DeepEqual(attr, expected); !ok {
		t.Fatalf("expected %+v, got %+v", expected, attr)
	}
}
