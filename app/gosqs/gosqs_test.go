package gosqs

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/p4tin/goaws/app"
)

func TestListQueues_POST_NoQueues(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ListQueues)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := "<ListQueuesResult></ListQueuesResult>"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestListQueues_POST_Success(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ListQueues)

	app.SyncQueues.Queues["foo"] = &app.Queue{Name: "foo", URL: "http://:/queue/foo"}
	app.SyncQueues.Queues["bar"] = &app.Queue{Name: "bar", URL: "http://:/queue/bar"}
	app.SyncQueues.Queues["foobar"] = &app.Queue{Name: "foobar", URL: "http://:/queue/foobar"}

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := "<QueueUrl>http://:/queue/bar</QueueUrl>"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}

	// Filter lists by the given QueueNamePrefix
	form := url.Values{}
	form.Add("QueueNamePrefix", "fo")
	req, _ = http.NewRequest("POST", "/", nil)
	req.PostForm = form
	rr = httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	unexpected := "<QueueUrl>http://:/queue/bar</QueueUrl>"
	if strings.Contains(rr.Body.String(), unexpected) {
		t.Errorf("handler returned unexpected body: got %v",
			rr.Body.String())
	}
}

func TestCreateQueuehandler_POST_CreateQueue(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	queueName := "UnitTestQueue1"
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "UnitTestQueue1")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "60")
	form.Add("Attribute.2.Name", "MaximumMessageSize")
	form.Add("Attribute.2.Value", "2048")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(CreateQueue)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := queueName
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
	expectedQueue := &app.Queue{
		Name:               queueName,
		URL:                "http://://" + queueName,
		Arn:                "arn:aws:sqs:::" + queueName,
		TimeoutSecs:        60,
		MaximumMessageSize: 2048,
		Duplicates:         make(map[string]time.Time),
	}
	actualQueue := app.SyncQueues.Queues[queueName]
	if !reflect.DeepEqual(expectedQueue, actualQueue) {
		t.Fatalf("expected %+v, got %+v", expectedQueue, actualQueue)
	}
}

func TestCreateFIFOQueuehandler_POST_CreateQueue(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	queueName := "UnitTestQueue1.fifo"
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "UnitTestQueue1.fifo")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "60")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(CreateQueue)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := queueName
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
	expectedQueue := &app.Queue{
		Name:        queueName,
		URL:         "http://://" + queueName,
		Arn:         "arn:aws:sqs:::" + queueName,
		TimeoutSecs: 60,
		IsFIFO:      true,
		Duplicates:  make(map[string]time.Time),
	}
	actualQueue := app.SyncQueues.Queues[queueName]
	if !reflect.DeepEqual(expectedQueue, actualQueue) {
		t.Fatalf("expected %+v, got %+v", expectedQueue, actualQueue)
	}
}

func TestSendMessage_MaximumMessageSize_Success(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["test_max_message_size"] =
		&app.Queue{Name: "test_max_message_size", MaximumMessageSize: 100}

	form := url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/test_max_message_size")
	form.Add("MessageBody", "test%20message%20body%201")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessage)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := "MD5OfMessageBody"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessage_MaximumMessageSize_MessageTooBig(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["test_max_message_size"] =
		&app.Queue{Name: "test_max_message_size", MaximumMessageSize: 10}

	form := url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/test_max_message_size")
	form.Add("MessageBody", "test%20message%20body%201")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessage)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "MessageTooBig"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendQueue_POST_NonExistant(t *testing.T) {
	// Create a request to pass to our handler. We don't have any query parameters for now, so we'll
	// pass 'nil' as the third parameter.
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/NON-EXISTANT")
	form.Add("MessageBody", "Test123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessage)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "NonExistentQueue"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatch_POST_QueueNotFound(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("SendMessageBatchRequestEntry.1.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.1.MessageBody", "test%20message%20body%201")
	form.Add("SendMessageBatchRequestEntry.2.Id", "test_msg_002")
	form.Add("SendMessageBatchRequestEntry.2.MessageBody", "test%20message%20body%202")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "NonExistentQueue"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatch_POST_NoEntry(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing"] = &app.Queue{Name: "testing"}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "EmptyBatchRequest"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}

	req, _ = http.NewRequest("POST", "/", nil)
	form.Add("SendMessageBatchRequestEntry", "")
	req.PostForm = form

	rr = httptest.NewRecorder()

	handler.ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatch_POST_IdNotDistinct(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing"] = &app.Queue{Name: "testing"}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("SendMessageBatchRequestEntry.1.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.1.MessageBody", "test%20message%20body%201")
	form.Add("SendMessageBatchRequestEntry.2.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.2.MessageBody", "test%20message%20body%202")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "BatchEntryIdsNotDistinct"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatch_POST_TooManyEntries(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing"] = &app.Queue{Name: "testing"}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("SendMessageBatchRequestEntry.1.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.1.MessageBody", "test%20message%20body%201")
	form.Add("SendMessageBatchRequestEntry.2.Id", "test_msg_002")
	form.Add("SendMessageBatchRequestEntry.2.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.3.Id", "test_msg_003")
	form.Add("SendMessageBatchRequestEntry.3.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.4.Id", "test_msg_004")
	form.Add("SendMessageBatchRequestEntry.4.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.5.Id", "test_msg_005")
	form.Add("SendMessageBatchRequestEntry.5.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.6.Id", "test_msg_006")
	form.Add("SendMessageBatchRequestEntry.6.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.7.Id", "test_msg_007")
	form.Add("SendMessageBatchRequestEntry.7.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.8.Id", "test_msg_008")
	form.Add("SendMessageBatchRequestEntry.8.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.9.Id", "test_msg_009")
	form.Add("SendMessageBatchRequestEntry.9.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.10.Id", "test_msg_010")
	form.Add("SendMessageBatchRequestEntry.10.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.11.Id", "test_msg_011")
	form.Add("SendMessageBatchRequestEntry.11.MessageBody", "test%20message%20body%202")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "TooManyEntriesInBatchRequest"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatch_POST_Success(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing"] = &app.Queue{Name: "testing"}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("SendMessageBatchRequestEntry.1.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.1.MessageBody", "test%20message%20body%201")
	form.Add("SendMessageBatchRequestEntry.2.Id", "test_msg_002")
	form.Add("SendMessageBatchRequestEntry.2.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Name", "test_attribute_name_1")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.StringValue", "test_attribute_value_1")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.DataType", "String")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := "<MD5OfMessageBody>1c538b76fce1a234bce865025c02b042</MD5OfMessageBody>"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendMessageBatchToFIFOQueue_POST_Success(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing.fifo"] = &app.Queue{
		Name:   "testing.fifo",
		IsFIFO: true,
	}

	form := url.Values{}
	form.Add("Action", "SendMessageBatch")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing.fifo")
	form.Add("SendMessageBatchRequestEntry.1.Id", "test_msg_001")
	form.Add("SendMessageBatchRequestEntry.1.MessageGroupId", "GROUP-X")
	form.Add("SendMessageBatchRequestEntry.1.MessageBody", "test%20message%20body%201")
	form.Add("SendMessageBatchRequestEntry.2.Id", "test_msg_002")
	form.Add("SendMessageBatchRequestEntry.2.MessageGroupId", "GROUP-X")
	form.Add("SendMessageBatchRequestEntry.2.MessageBody", "test%20message%20body%202")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Name", "test_attribute_name_1")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.StringValue", "test_attribute_value_1")
	form.Add("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.DataType", "String")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SendMessageBatch)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := "<MD5OfMessageBody>1c538b76fce1a234bce865025c02b042</MD5OfMessageBody>"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestChangeMessageVisibility_POST_SUCCESS(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["testing"] = &app.Queue{Name: "testing"}
	app.SyncQueues.Queues["testing"].Messages = []app.Message{{
		MessageBody:   []byte("test1"),
		ReceiptHandle: "123",
	}}

	form := url.Values{}
	form.Add("Action", "ChangeMessageVisibility")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing")
	form.Add("VisibilityTimeout", "0")
	form.Add("ReceiptHandle", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(ChangeMessageVisibility)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// Check the response body is what we expect.
	expected := `<ChangeMessageVisibilityResult xmlns="http://queue.amazonaws.com/doc/2012-11-05/">`
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestRequeueing_VisibilityTimeoutExpires(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "requeue")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue")
	form.Add("MessageBody", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}

	// try to receive another message.
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}
	time.Sleep(2 * time.Second)

	// message needs to be requeued
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}
	done <- struct{}{}
}

func TestRequeueing_ResetVisibilityTimeout(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "requeue-reset")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "10")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset")
	form.Add("MessageBody", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}

	resp := app.ReceiveMessageResponse{}
	err = xml.Unmarshal(rr.Body.Bytes(), &resp)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %s", err)
	}
	receiptHandle := resp.Result.Message[0].ReceiptHandle

	// try to receive another message.
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}

	// reset message visibility timeout to requeue it
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ChangeMessageVisibility")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset")
	form.Add("VisibilityTimeout", "0")
	form.Add("ReceiptHandle", receiptHandle)
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ChangeMessageVisibility).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// message needs to be requeued
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}
	done <- struct{}{}
}

func TestDeadLetterQueue(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	deadLetterQueue := &app.Queue{
		Name:     "failed-messages",
		Messages: []app.Message{},
	}
	app.SyncQueues.Lock()
	app.SyncQueues.Queues["failed-messages"] = deadLetterQueue
	app.SyncQueues.Unlock()
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "testing-deadletter")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "1")
	form.Add("Attribute.2.Name", "RedrivePolicy")
	form.Add("Attribute.2.Value", `{"maxReceiveCount": 1, "deadLetterTargetArn":"arn:aws:sqs::000000000000:failed-messages"}`)
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing-deadletter")
	form.Add("MessageBody", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/testing-deadletter")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}

	time.Sleep(2 * time.Second)

	// receive the message one more time
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}
	time.Sleep(2 * time.Second)

	// another receive attempt
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}
	if len(deadLetterQueue.Messages) == 0 {
		t.Fatal("expected a message")
	}
}

func TestReceiveMessageWaitTimeEnforced(t *testing.T) {
	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "waiting-queue")
	form.Add("Attribute.1.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.1.Value", "2")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message ensure delay
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/waiting-queue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()

	start := time.Now()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	elapsed := time.Since(start)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}
	if elapsed < 2*time.Second {
		t.Fatal("handler didn't wait ReceiveMessageWaitTimeSeconds")
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/waiting-queue")
	form.Add("MessageBody", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/waiting-queue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()

	start = time.Now()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	elapsed = time.Since(start)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}
	if elapsed > 1*time.Second {
		t.Fatal("handler waited when message was available, expected not to wait")
	}
}

func TestReceiveMessage_CanceledByClient(t *testing.T) {
	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "cancel-queue")
	form.Add("Attribute.1.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.1.Value", "20")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	var wg sync.WaitGroup
	ctx, cancelReceive := context.WithCancel(context.Background())

	wg.Add(1)
	go func() {
		defer wg.Done()
		// receive message (that will be canceled)
		req, err := http.NewRequest("POST", "/", nil)
		req = req.WithContext(ctx)
		if err != nil {
			t.Fatal(err)
		}

		form := url.Values{}
		form.Add("Action", "ReceiveMessage")
		form.Add("QueueUrl", "http://localhost:4100/queue/cancel-queue")
		form.Add("Version", "2012-11-05")
		req.PostForm = form

		rr := httptest.NewRecorder()
		http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got \n%v want %v",
				status, http.StatusOK)
		}

		if ok := strings.Contains(rr.Body.String(), "12345"); ok {
			t.Fatal("expecting this ReceiveMessage() to not pickup this message as it should canceled before the Send()")
		}
	}()
	time.Sleep(100 * time.Millisecond) // let enought time for the Receive go to wait mode
	cancelReceive()                    // cancel the first ReceiveMessage(), make sure it will not pickup the sent message below
	time.Sleep(5 * time.Millisecond)

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/cancel-queue")
	form.Add("MessageBody", "12345")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/cancel-queue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()

	start := time.Now()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	elapsed := time.Since(start)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "12345"); !ok {
		t.Fatal("handler should return a message")
	}
	if elapsed > 1*time.Second {
		t.Fatal("handler waited when message was available, expected not to wait")
	}

	if timedout := waitTimeout(&wg, 2*time.Second); timedout {
		t.Errorf("expected ReceiveMessage() in goroutine to exit quickly due to cancelReceive() called")
	}
}

func TestReceiveMessage_WithConcurrentDeleteQueue(t *testing.T) {
	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "waiting-queue")
	form.Add("Attribute.1.Name", "ReceiveMessageWaitTimeSeconds")
	form.Add("Attribute.1.Value", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		// receive message
		req, err := http.NewRequest("POST", "/", nil)
		if err != nil {
			t.Fatal(err)
		}

		form := url.Values{}
		form.Add("Action", "ReceiveMessage")
		form.Add("QueueUrl", "http://localhost:4100/queue/waiting-queue")
		form.Add("Version", "2012-11-05")
		req.PostForm = form

		rr := httptest.NewRecorder()

		http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

		// Check the status code is what we expect.
		if status := rr.Code; status != http.StatusBadRequest {
			t.Errorf("handler returned wrong status code: got %v want %v",
				status, http.StatusBadRequest)
		}

		// Check the response body is what we expect.
		expected := "QueueNotFound"
		if !strings.Contains(rr.Body.String(), "Not Found") {
			t.Errorf("handler returned unexpected body: got %v want %v",
				rr.Body.String(), expected)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond) // 10ms to let the ReceiveMessage() block
		// delete queue message
		req, err := http.NewRequest("POST", "/", nil)
		if err != nil {
			t.Fatal(err)
		}
		form := url.Values{}
		form.Add("Action", "DeleteQueue")
		form.Add("QueueUrl", "http://localhost:4100/queue/waiting-queue")
		form.Add("Version", "2012-11-05")
		req.PostForm = form

		rr := httptest.NewRecorder()
		http.HandlerFunc(DeleteQueue).ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got \n%v want %v",
				status, http.StatusOK)
		}
	}()

	if timedout := waitTimeout(&wg, 2*time.Second); timedout {
		t.Errorf("concurrent handlers timeout, expecting both to return within timeout")
	}
}

func TestReceiveMessageDelaySeconds(t *testing.T) {
	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "delay-seconds-queue")
	form.Add("Attribute.1.Name", "DelaySeconds")
	form.Add("Attribute.1.Value", "2")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/delay-seconds-queue")
	form.Add("MessageBody", "1")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message before delay is up
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/delay-seconds-queue")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}

	// receive message with wait should return after delay
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/delay-seconds-queue")
	form.Add("WaitTimeSeconds", "10")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	start := time.Now()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	elapsed := time.Since(start)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if elapsed < 1*time.Second {
		t.Errorf("handler didn't wait at all")
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Errorf("handler should return a message")
	}
	if elapsed > 4*time.Second {
		t.Errorf("handler didn't need to wait all WaitTimeSeconds=10, only DelaySeconds=2")
	}
}

func TestSetQueueAttributes_POST_QueueNotFound(t *testing.T) {
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "SetQueueAttributes")
	form.Add("QueueUrl", "http://localhost:4100/queue/not-existing")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	// We create a ResponseRecorder (which satisfies http.ResponseWriter) to record the response.
	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(SetQueueAttributes)

	// Our handlers satisfy http.Handler, so we can call their ServeHTTP method
	// directly and pass in our Request and ResponseRecorder.
	handler.ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusBadRequest {
		t.Errorf("handler returned wrong status code: got %v want %v",
			status, http.StatusBadRequest)
	}

	// Check the response body is what we expect.
	expected := "NonExistentQueue"
	if !strings.Contains(rr.Body.String(), expected) {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestSendingAndReceivingFromFIFOQueueReturnsSameMessageOnError(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "requeue-reset.fifo")
	form.Add("Attribute.1.Name", "VisibilityTimeout")
	form.Add("Attribute.1.Value", "2")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
	form.Add("MessageBody", "1")
	form.Add("MessageGroupId", "GROUP-X")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
	form.Add("MessageBody", "2")
	form.Add("MessageGroupId", "GROUP-X")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Fatal("handler should return a message")
	}

	resp := app.ReceiveMessageResponse{}
	err = xml.Unmarshal(rr.Body.Bytes(), &resp)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %s", err)
	}
	receiptHandleFirst := resp.Result.Message[0].ReceiptHandle
	if string(resp.Result.Message[0].Body) != "1" {
		t.Fatalf("should have received body 1: %s", err)
	}

	// try to receive another message and we should get none
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}

	if len(app.SyncQueues.Queues["requeue-reset.fifo"].FIFOMessages) != 1 {
		t.Fatal("there should be only 1 group locked")
	}

	if app.SyncQueues.Queues["requeue-reset.fifo"].FIFOMessages["GROUP-X"] != 0 {
		t.Fatal("there should be GROUP-X locked")
	}

	// remove message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "DeleteMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
	form.Add("ReceiptHandle", receiptHandleFirst)
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(DeleteMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["requeue-reset.fifo"].Messages) != 1 {
		t.Fatal("there should be only 1 message in queue")
	}

	// receive message - loop until visibility timeouts
	for {
		req, err = http.NewRequest("POST", "/", nil)
		if err != nil {
			t.Fatal(err)
		}

		form = url.Values{}
		form.Add("Action", "ReceiveMessage")
		form.Add("QueueUrl", "http://localhost:4100/queue/requeue-reset.fifo")
		form.Add("Version", "2012-11-05")
		req.PostForm = form

		rr = httptest.NewRecorder()
		http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)

		if status := rr.Code; status != http.StatusOK {
			t.Errorf("handler returned wrong status code: got \n%v want %v",
				status, http.StatusOK)
		}
		if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
			continue
		}

		resp = app.ReceiveMessageResponse{}
		err = xml.Unmarshal(rr.Body.Bytes(), &resp)
		if err != nil {
			t.Fatalf("unexpected unmarshal error: %s", err)
		}
		if string(resp.Result.Message[0].Body) != "2" {
			t.Fatalf("should have received body 2: %s", err)
		}
		break
	}

	done <- struct{}{}
}

func TestSendMessage_POST_DuplicatationNotAppliedToStandardQueue(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "stantdard-testing")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/stantdard-testing")
	form.Add("MessageBody", "Test1")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["stantdard-testing"].Messages) == 0 {
		t.Fatal("there should be 1 message in queue")
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/stantdard-testing")
	form.Add("MessageBody", "Test2")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["stantdard-testing"].Messages) == 1 {
		t.Fatal("there should be 2 messages in queue")
	}
}

func TestSendMessage_POST_DuplicatationDisabledOnFifoQueue(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "no-dup-testing.fifo")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/no-dup-testing.fifo")
	form.Add("MessageBody", "Test1")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["no-dup-testing.fifo"].Messages) == 0 {
		t.Fatal("there should be 1 message in queue")
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/no-dup-testing.fifo")
	form.Add("MessageBody", "Test2")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["no-dup-testing.fifo"].Messages) != 2 {
		t.Fatal("there should be 2 message in queue")
	}
}

func TestSendMessage_POST_DuplicatationEnabledOnFifoQueue(t *testing.T) {
	done := make(chan struct{}, 0)
	go PeriodicTasks(1*time.Second, done)

	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "dup-testing.fifo")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}

	app.SyncQueues.Queues["dup-testing.fifo"].EnableDuplicates = true

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/dup-testing.fifo")
	form.Add("MessageBody", "Test1")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["dup-testing.fifo"].Messages) == 0 {
		t.Fatal("there should be 1 message in queue")
	}

	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/dup-testing.fifo")
	form.Add("MessageBody", "Test2")
	form.Add("MessageDeduplicationId", "123")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)

	// Check the status code is what we expect.
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if len(app.SyncQueues.Queues["dup-testing.fifo"].Messages) != 1 {
		t.Fatal("there should be 1 message in queue")
	}
	if body := app.SyncQueues.Queues["dup-testing.fifo"].Messages[0].MessageBody; string(body) == "Test2" {
		t.Fatal("duplicate message should not be added to queue")
	}
}

func TestSendMessage_POST_DelaySeconds(t *testing.T) {
	// create a queue
	req, err := http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form := url.Values{}
	form.Add("Action", "CreateQueue")
	form.Add("QueueName", "sendmessage-delay")
	form.Add("Version", "2012-11-05")
	req.PostForm = form

	rr := httptest.NewRecorder()
	http.HandlerFunc(CreateQueue).ServeHTTP(rr, req)

	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// send a message
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "SendMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/sendmessage-delay")
	form.Add("MessageBody", "1")
	form.Add("DelaySeconds", "2")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	http.HandlerFunc(SendMessage).ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}

	// receive message before delay is up
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/sendmessage-delay")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); ok {
		t.Fatal("handler should not return a message")
	}

	// receive message with wait should return after delay
	req, err = http.NewRequest("POST", "/", nil)
	if err != nil {
		t.Fatal(err)
	}
	form = url.Values{}
	form.Add("Action", "ReceiveMessage")
	form.Add("QueueUrl", "http://localhost:4100/queue/sendmessage-delay")
	form.Add("WaitTimeSeconds", "10")
	form.Add("Version", "2012-11-05")
	req.PostForm = form
	rr = httptest.NewRecorder()
	start := time.Now()
	http.HandlerFunc(ReceiveMessage).ServeHTTP(rr, req)
	elapsed := time.Since(start)
	if status := rr.Code; status != http.StatusOK {
		t.Errorf("handler returned wrong status code: got \n%v want %v",
			status, http.StatusOK)
	}
	if elapsed < 1*time.Second {
		t.Errorf("handler didn't wait at all")
	}
	if ok := strings.Contains(rr.Body.String(), "<Message>"); !ok {
		t.Errorf("handler should return a message")
	}
	if elapsed > 4*time.Second {
		t.Errorf("handler didn't need to wait all WaitTimeSeconds=10, only DelaySeconds=2")
	}
}

// waitTimeout waits for the waitgroup for the specified max timeout.
// Returns true if waiting timed out.
// credits: https://stackoverflow.com/questions/32840687/timeout-for-waitgroup-wait
func waitTimeout(wg *sync.WaitGroup, timeout time.Duration) bool {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		return true // timed out
	}
}
