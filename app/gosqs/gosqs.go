package gosqs

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gorilla/mux"
	"github.com/p4tin/goaws/app"
	"github.com/p4tin/goaws/app/common"
)

func init() {
	app.SyncQueues.Queues = make(map[string]*app.Queue)

	app.SqsErrors = make(map[string]app.SqsErrorType)
	err1 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "Not Found", Code: "AWS.SimpleQueueService.NonExistentQueue", Message: "The specified queue does not exist for this wsdl version."}
	app.SqsErrors["QueueNotFound"] = err1
	err2 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "Duplicate", Code: "AWS.SimpleQueueService.QueueExists", Message: "The specified queue already exists."}
	app.SqsErrors["QueueExists"] = err2
	err3 := app.SqsErrorType{HttpError: http.StatusNotFound, Type: "Not Found", Code: "AWS.SimpleQueueService.QueueExists", Message: "The specified queue does not contain the message specified."}
	app.SqsErrors["MessageDoesNotExist"] = err3
	err4 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "GeneralError", Code: "AWS.SimpleQueueService.GeneralError", Message: "General Error."}
	app.SqsErrors["GeneralError"] = err4
	err5 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "TooManyEntriesInBatchRequest", Code: "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", Message: "Maximum number of entries per request are 10."}
	app.SqsErrors["TooManyEntriesInBatchRequest"] = err5
	err6 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "BatchEntryIdsNotDistinct", Code: "AWS.SimpleQueueService.BatchEntryIdsNotDistinct", Message: "Two or more batch entries in the request have the same Id."}
	app.SqsErrors["BatchEntryIdsNotDistinct"] = err6
	err7 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "EmptyBatchRequest", Code: "AWS.SimpleQueueService.EmptyBatchRequest", Message: "The batch request doesn't contain any entries."}
	app.SqsErrors["EmptyBatchRequest"] = err7
	err8 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "ValidationError", Code: "AWS.SimpleQueueService.ValidationError", Message: "The visibility timeout is incorrect"}
	app.SqsErrors["InvalidVisibilityTimeout"] = err8
	err9 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "MessageNotInFlight", Code: "AWS.SimpleQueueService.MessageNotInFlight", Message: "The message referred to isn't in flight."}
	app.SqsErrors["MessageNotInFlight"] = err9
	err10 := app.SqsErrorType{HttpError: http.StatusBadRequest, Type: "MessageTooBig", Code: "InvalidMessageContents", Message: "The message size exceeds the limit."}
	app.SqsErrors["MessageTooBig"] = err10
	app.SqsErrors[ErrInvalidParameterValue.Type] = *ErrInvalidParameterValue
	app.SqsErrors[ErrInvalidAttributeValue.Type] = *ErrInvalidAttributeValue
}

func PeriodicTasks(d time.Duration, quit <-chan struct{}) {
	ticker := time.NewTicker(d)
	for {
		select {
		case <-ticker.C:
			app.SyncQueues.Lock()
			for j := range app.SyncQueues.Queues {
				queue := app.SyncQueues.Queues[j]

				log.Debugf("Queue [%s] length [%d]", queue.Name, len(queue.Messages))
				for i := 0; i < len(queue.Messages); i++ {
					msg := &queue.Messages[i]

					// Reset deduplication period
					for dedupId, startTime := range queue.Duplicates {
						if time.Now().After(startTime.Add(app.DeduplicationPeriod)) {
							log.Debugf("deduplication period for message with deduplicationId [%s] expired", dedupId)
							delete(queue.Duplicates, dedupId)
						}
					}

					if msg.ReceiptHandle != "" {
						if msg.VisibilityTimeout.Before(time.Now()) {
							log.Debugf("Making message visible again %s", msg.ReceiptHandle)
							queue.UnlockGroup(msg.GroupID)
							msg.ReceiptHandle = ""
							msg.ReceiptTime = time.Now().UTC()
							msg.Retry++
							if queue.MaxReceiveCount > 0 &&
								queue.DeadLetterQueue != nil &&
								msg.Retry > queue.MaxReceiveCount {
								queue.DeadLetterQueue.Messages = append(queue.DeadLetterQueue.Messages, *msg)
								queue.Messages = append(queue.Messages[:i], queue.Messages[i+1:]...)
								i++
							}
						}
					}
				}
			}
			app.SyncQueues.Unlock()
		case <-quit:
			ticker.Stop()
			return
		}
	}
}

func ListQueues(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	respStruct := app.ListQueuesResponse{}
	respStruct.Xmlns = "http://queue.amazonaws.com/doc/2012-11-05/"
	respStruct.Metadata = app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}
	respStruct.Result.QueueUrl = make([]string, 0)
	queueNamePrefix := req.FormValue("QueueNamePrefix")

	log.Println("Listing Queues")
	for _, queue := range app.SyncQueues.Queues {
		app.SyncQueues.Lock()
		if strings.HasPrefix(queue.Name, queueNamePrefix) {
			respStruct.Result.QueueUrl = append(respStruct.Result.QueueUrl, queue.URL)
		}
		app.SyncQueues.Unlock()
	}
	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func CreateQueue(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	queueName := req.FormValue("QueueName")

	queueUrl := "http://" + app.CurrentEnvironment.Host + ":" + app.CurrentEnvironment.Port +
		"/" + app.CurrentEnvironment.AccountID + "/" + queueName
	if app.CurrentEnvironment.Region != "" {
		queueUrl = "http://" + app.CurrentEnvironment.Region + "." + app.CurrentEnvironment.Host + ":" +
			app.CurrentEnvironment.Port + "/" + app.CurrentEnvironment.AccountID + "/" + queueName
	}
	queueArn := "arn:aws:sqs:" + app.CurrentEnvironment.Region + ":" + app.CurrentEnvironment.AccountID + ":" + queueName

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		log.Println("Creating Queue:", queueName)
		queue := &app.Queue{
			Name:                queueName,
			URL:                 queueUrl,
			Arn:                 queueArn,
			TimeoutSecs:         app.CurrentEnvironment.QueueAttributeDefaults.VisibilityTimeout,
			ReceiveWaitTimeSecs: app.CurrentEnvironment.QueueAttributeDefaults.ReceiveMessageWaitTimeSeconds,
			MaximumMessageSize:  app.CurrentEnvironment.QueueAttributeDefaults.MaximumMessageSize,
			IsFIFO:              app.HasFIFOQueueName(queueName),
			EnableDuplicates:    app.CurrentEnvironment.EnableDuplicates,
			Duplicates:          make(map[string]time.Time),
		}
		if err := validateAndSetQueueAttributes(queue, req.Form); err != nil {
			createErrorResponse(w, req, err.Error())
			return
		}
		app.SyncQueues.Lock()
		app.SyncQueues.Queues[queueName] = queue
		app.SyncQueues.Unlock()
	}

	respStruct := app.CreateQueueResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.CreateQueueResult{QueueUrl: queueUrl}, app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func SendMessage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	req.ParseForm()
	messageBody := req.FormValue("MessageBody")
	messageGroupID := req.FormValue("MessageGroupId")
	messageDeduplicationID := req.FormValue("MessageDeduplicationId")
	messageAttributes := extractMessageAttributes(req, "")

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())

	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		// Queue does not exist
		createErrorResponse(w, req, "QueueNotFound")
		return
	}

	if app.SyncQueues.Queues[queueName].MaximumMessageSize > 0 &&
		len(messageBody) > app.SyncQueues.Queues[queueName].MaximumMessageSize {
		// Message size is too big
		createErrorResponse(w, req, "MessageTooBig")
		return
	}

	delaySecs := app.SyncQueues.Queues[queueName].DelaySecs
	if mv := req.FormValue("DelaySeconds"); mv != "" {
		delaySecs, _ = strconv.Atoi(mv)
	}

	log.Println("Putting Message in Queue:", queueName)
	msg := app.Message{MessageBody: []byte(messageBody)}
	if len(messageAttributes) > 0 {
		msg.MessageAttributes = messageAttributes
		msg.MD5OfMessageAttributes = common.HashAttributes(messageAttributes)
	}
	msg.MD5OfMessageBody = common.GetMD5Hash(messageBody)
	msg.Uuid, _ = common.NewUUID()
	msg.GroupID = messageGroupID
	msg.DeduplicationID = messageDeduplicationID
	msg.SentTime = time.Now()
	msg.DelaySecs = delaySecs

	app.SyncQueues.Lock()
	fifoSeqNumber := ""
	if app.SyncQueues.Queues[queueName].IsFIFO {
		fifoSeqNumber = app.SyncQueues.Queues[queueName].NextSequenceNumber(messageGroupID)
	}

	if !app.SyncQueues.Queues[queueName].IsDuplicate(messageDeduplicationID) {
		app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages, msg)
	} else {
		log.Debugf("Message with deduplicationId [%s] in queue [%s] is duplicate ", messageDeduplicationID, queueName)
	}

	app.SyncQueues.Queues[queueName].InitDuplicatation(messageDeduplicationID)
	app.SyncQueues.Unlock()
	log.Infof("%s: Queue: %s, Message: %s\n", time.Now().Format("2006-01-02 15:04:05"), queueName, msg.MessageBody)

	respStruct := app.SendMessageResponse{
		Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/",
		Result: app.SendMessageResult{
			MD5OfMessageAttributes: msg.MD5OfMessageAttributes,
			MD5OfMessageBody:       msg.MD5OfMessageBody,
			MessageId:              msg.Uuid,
			SequenceNumber:         fifoSeqNumber,
		},
		Metadata: app.ResponseMetadata{
			RequestId: "00000000-0000-0000-0000-000000000000",
		},
	}

	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

type SendEntry struct {
	Id                     string
	MessageBody            string
	MessageAttributes      map[string]app.MessageAttributeValue
	MessageGroupId         string
	MessageDeduplicationId string
}

func SendMessageBatch(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	req.ParseForm()

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())
	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		createErrorResponse(w, req, "QueueNotFound")
		return
	}

	sendEntries := []SendEntry{}

	for k, v := range req.Form {
		keySegments := strings.Split(k, ".")
		if keySegments[0] == "SendMessageBatchRequestEntry" {
			if len(keySegments) < 3 {
				createErrorResponse(w, req, "EmptyBatchRequest")
				return
			}
			keyIndex, err := strconv.Atoi(keySegments[1])
			if err != nil {
				createErrorResponse(w, req, "Error")
				return
			}

			if len(sendEntries) < keyIndex {
				newSendEntries := make([]SendEntry, keyIndex)
				copy(newSendEntries, sendEntries)
				sendEntries = newSendEntries
			}

			if keySegments[2] == "Id" {
				sendEntries[keyIndex-1].Id = v[0]
			}

			if keySegments[2] == "MessageBody" {
				sendEntries[keyIndex-1].MessageBody = v[0]
			}

			if keySegments[2] == "MessageGroupId" {
				sendEntries[keyIndex-1].MessageGroupId = v[0]
			}

			if keySegments[2] == "MessageGroupId" {
				sendEntries[keyIndex-1].MessageGroupId = v[0]
			}

			if keySegments[2] == "MessageDeduplicationId" {
				sendEntries[keyIndex-1].MessageDeduplicationId = v[0]
			}
		}
	}

	if len(sendEntries) == 0 {
		createErrorResponse(w, req, "EmptyBatchRequest")
		return
	}

	if len(sendEntries) > 10 {
		createErrorResponse(w, req, "TooManyEntriesInBatchRequest")
		return
	}
	ids := map[string]struct{}{}
	for _, v := range sendEntries {
		if _, ok := ids[v.Id]; ok {
			createErrorResponse(w, req, "BatchEntryIdsNotDistinct")
			return
		}
		ids[v.Id] = struct{}{}
	}

	sentEntries := make([]app.SendMessageBatchResultEntry, 0)
	log.Println("Putting Message in Queue:", queueName)
	for _, sendEntry := range sendEntries {
		msg := app.Message{MessageBody: []byte(sendEntry.MessageBody)}
		if len(sendEntry.MessageAttributes) > 0 {
			msg.MessageAttributes = sendEntry.MessageAttributes
			msg.MD5OfMessageAttributes = common.HashAttributes(sendEntry.MessageAttributes)
		}
		msg.MD5OfMessageBody = common.GetMD5Hash(sendEntry.MessageBody)
		msg.GroupID = sendEntry.MessageGroupId
		msg.DeduplicationID = sendEntry.MessageDeduplicationId
		msg.Uuid, _ = common.NewUUID()
		msg.SentTime = time.Now()
		app.SyncQueues.Lock()
		fifoSeqNumber := ""
		if app.SyncQueues.Queues[queueName].IsFIFO {
			fifoSeqNumber = app.SyncQueues.Queues[queueName].NextSequenceNumber(sendEntry.MessageGroupId)
		}

		if !app.SyncQueues.Queues[queueName].IsDuplicate(sendEntry.MessageDeduplicationId) {
			app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages, msg)
		} else {
			log.Debugf("Message with deduplicationId [%s] in queue [%s] is duplicate ", sendEntry.MessageDeduplicationId, queueName)
		}

		app.SyncQueues.Queues[queueName].InitDuplicatation(sendEntry.MessageDeduplicationId)

		app.SyncQueues.Unlock()
		se := app.SendMessageBatchResultEntry{
			Id:                     sendEntry.Id,
			MessageId:              msg.Uuid,
			MD5OfMessageBody:       msg.MD5OfMessageBody,
			MD5OfMessageAttributes: msg.MD5OfMessageAttributes,
			SequenceNumber:         fifoSeqNumber,
		}
		sentEntries = append(sentEntries, se)
		log.Infof("%s: Queue: %s, Message: %s\n", time.Now().Format("2006-01-02 15:04:05"), queueName, msg.MessageBody)
	}

	respStruct := app.SendMessageBatchResponse{
		"http://queue.amazonaws.com/doc/2012-11-05/",
		app.SendMessageBatchResult{Entry: sentEntries},
		app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000001"}}

	enc := xml.NewEncoder(w)
	enc.Indent(" ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func ReceiveMessage(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	req.ParseForm()

	waitTimeSeconds := 0
	wts := req.FormValue("WaitTimeSeconds")
	if wts != "" {
		waitTimeSeconds, _ = strconv.Atoi(wts)
	}
	maxNumberOfMessages := 1
	mom := req.FormValue("MaxNumberOfMessages")
	if mom != "" {
		maxNumberOfMessages, _ = strconv.Atoi(mom)
	}

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())

	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		createErrorResponse(w, req, "QueueNotFound")
		return
	}

	var messages []*app.ResultMessage
	//	respMsg := ResultMessage{}
	respStruct := app.ReceiveMessageResponse{}

	if waitTimeSeconds == 0 {
		app.SyncQueues.RLock()
		waitTimeSeconds = app.SyncQueues.Queues[queueName].ReceiveWaitTimeSecs
		app.SyncQueues.RUnlock()
	}

	loops := waitTimeSeconds * 10
	for loops > 0 {
		app.SyncQueues.RLock()
		_, queueFound := app.SyncQueues.Queues[queueName]
		if !queueFound {
			app.SyncQueues.RUnlock()
			createErrorResponse(w, req, "QueueNotFound")
			return
		}
		messageFound := len(app.SyncQueues.Queues[queueName].Messages)-numberOfHiddenMessagesInQueue(*app.SyncQueues.Queues[queueName]) != 0
		app.SyncQueues.RUnlock()
		if !messageFound {
			continueTimer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-req.Context().Done():
				continueTimer.Stop()
				return // client gave up
			case <-continueTimer.C:
				continueTimer.Stop()
			}
			loops--
		} else {
			break
		}

	}
	log.Println("Getting Message from Queue:", queueName)

	app.SyncQueues.Lock() // Lock the Queues
	if len(app.SyncQueues.Queues[queueName].Messages) > 0 {
		numMsg := 0
		messages = make([]*app.ResultMessage, 0)
		for i := range app.SyncQueues.Queues[queueName].Messages {
			if numMsg >= maxNumberOfMessages {
				break
			}

			if app.SyncQueues.Queues[queueName].Messages[i].ReceiptHandle != "" {
				continue
			}

			uuid, _ := common.NewUUID()

			msg := &app.SyncQueues.Queues[queueName].Messages[i]
			if !msg.IsReadyForReceipt() {
				continue
			}
			msg.ReceiptHandle = msg.Uuid + "#" + uuid
			msg.ReceiptTime = time.Now().UTC()
			msg.VisibilityTimeout = time.Now().Add(time.Duration(app.SyncQueues.Queues[queueName].TimeoutSecs) * time.Second)

			if app.SyncQueues.Queues[queueName].IsFIFO {
				// If we got messages here it means we have not processed it yet, so get next
				if app.SyncQueues.Queues[queueName].IsLocked(msg.GroupID) {
					continue
				}
				// Otherwise lock messages for group ID
				app.SyncQueues.Queues[queueName].LockGroup(msg.GroupID)
			}

			messages = append(messages, getMessageResult(msg))

			numMsg++
		}

		//		respMsg = ResultMessage{MessageId: messages.Uuid, ReceiptHandle: messages.ReceiptHandle, MD5OfBody: messages.MD5OfMessageBody, Body: messages.MessageBody, MD5OfMessageAttributes: messages.MD5OfMessageAttributes}
		respStruct = app.ReceiveMessageResponse{
			"http://queue.amazonaws.com/doc/2012-11-05/",
			app.ReceiveMessageResult{
				Message: messages,
			},
			app.ResponseMetadata{
				RequestId: "00000000-0000-0000-0000-000000000000",
			},
		}
	} else {
		log.Println("No messages in Queue:", queueName)
		respStruct = app.ReceiveMessageResponse{Xmlns: "http://queue.amazonaws.com/doc/2012-11-05/", Result: app.ReceiveMessageResult{}, Metadata: app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
	}
	app.SyncQueues.Unlock() // Unlock the Queues
	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func numberOfHiddenMessagesInQueue(queue app.Queue) int {
	num := 0
	for _, m := range queue.Messages {
		if m.ReceiptHandle != "" || m.DelaySecs > 0 && time.Now().Before(m.SentTime.Add(time.Duration(m.DelaySecs)*time.Second)) {
			num++
		}
	}
	return num
}

func ChangeMessageVisibility(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	vars := mux.Vars(req)

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())
	queueName := ""
	if queueUrl == "" {
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}
	receiptHandle := req.FormValue("ReceiptHandle")
	visibilityTimeout, err := strconv.Atoi(req.FormValue("VisibilityTimeout"))
	if err != nil {
		createErrorResponse(w, req, "ValidationError")
		return
	}
	if visibilityTimeout > 43200 {
		createErrorResponse(w, req, "ValidationError")
		return
	}

	if _, ok := app.SyncQueues.Queues[queueName]; !ok {
		createErrorResponse(w, req, "QueueNotFound")
		return
	}

	app.SyncQueues.Lock()
	messageFound := false
	for i := 0; i < len(app.SyncQueues.Queues[queueName].Messages); i++ {
		queue := app.SyncQueues.Queues[queueName]
		msgs := queue.Messages
		if msgs[i].ReceiptHandle == receiptHandle {
			timeout := app.SyncQueues.Queues[queueName].TimeoutSecs
			if visibilityTimeout == 0 {
				msgs[i].ReceiptTime = time.Now().UTC()
				msgs[i].ReceiptHandle = ""
				msgs[i].VisibilityTimeout = time.Now().Add(time.Duration(timeout) * time.Second)
				msgs[i].Retry++
				if queue.MaxReceiveCount > 0 &&
					queue.DeadLetterQueue != nil &&
					msgs[i].Retry > queue.MaxReceiveCount {
					queue.DeadLetterQueue.Messages = append(queue.DeadLetterQueue.Messages, msgs[i])
					queue.Messages = append(queue.Messages[:i], queue.Messages[i+1:]...)
					i++
				}
			} else {
				msgs[i].VisibilityTimeout = time.Now().Add(time.Duration(visibilityTimeout) * time.Second)
			}
			messageFound = true
			break
		}
	}
	app.SyncQueues.Unlock()
	if !messageFound {
		createErrorResponse(w, req, "MessageNotInFlight")
		return
	}

	respStruct := app.ChangeMessageVisibilityResult{
		"http://queue.amazonaws.com/doc/2012-11-05/",
		app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000001"}}

	enc := xml.NewEncoder(w)
	enc.Indent(" ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
		createErrorResponse(w, req, "GeneralError")
		return
	}
}

type DeleteEntry struct {
	Id            string
	ReceiptHandle string
	Error         string
	Deleted       bool
}

func DeleteMessageBatch(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/xml")
	req.ParseForm()

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())
	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	deleteEntries := []DeleteEntry{}

	for k, v := range req.Form {
		keySegments := strings.Split(k, ".")
		if keySegments[0] == "DeleteMessageBatchRequestEntry" {
			keyIndex, err := strconv.Atoi(keySegments[1])
			if err != nil {
				createErrorResponse(w, req, "Error")
				return
			}

			if len(deleteEntries) < keyIndex {
				newDeleteEntries := make([]DeleteEntry, keyIndex)
				copy(newDeleteEntries, deleteEntries)
				deleteEntries = newDeleteEntries
			}

			if keySegments[2] == "Id" {
				deleteEntries[keyIndex-1].Id = v[0]
			}

			if keySegments[2] == "ReceiptHandle" {
				deleteEntries[keyIndex-1].ReceiptHandle = v[0]
			}
		}
	}

	deletedEntries := make([]app.DeleteMessageBatchResultEntry, 0)

	app.SyncQueues.Lock()
	if _, ok := app.SyncQueues.Queues[queueName]; ok {
		for _, deleteEntry := range deleteEntries {
			for i, msg := range app.SyncQueues.Queues[queueName].Messages {
				if msg.ReceiptHandle == deleteEntry.ReceiptHandle {
					// Unlock messages for the group
					log.Printf("FIFO Queue %s unlocking group %s:", queueName, msg.GroupID)
					app.SyncQueues.Queues[queueName].UnlockGroup(msg.GroupID)
					app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages[:i], app.SyncQueues.Queues[queueName].Messages[i+1:]...)
					delete(app.SyncQueues.Queues[queueName].Duplicates, msg.DeduplicationID)

					deleteEntry.Deleted = true
					deletedEntry := app.DeleteMessageBatchResultEntry{Id: deleteEntry.Id}
					deletedEntries = append(deletedEntries, deletedEntry)
					break
				}
			}
		}
	}
	app.SyncQueues.Unlock()

	notFoundEntries := make([]app.BatchResultErrorEntry, 0)
	for _, deleteEntry := range deleteEntries {
		if deleteEntry.Deleted {
			notFoundEntries = append(notFoundEntries, app.BatchResultErrorEntry{
				Code:        "1",
				Id:          deleteEntry.Id,
				Message:     "Message not found",
				SenderFault: true})
		}
	}

	respStruct := app.DeleteMessageBatchResponse{
		"http://queue.amazonaws.com/doc/2012-11-05/",
		app.DeleteMessageBatchResult{Entry: deletedEntries, Error: notFoundEntries},
		app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000001"}}

	enc := xml.NewEncoder(w)
	enc.Indent(" ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func DeleteMessage(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")

	// Retrieve FormValues required
	receiptHandle := req.FormValue("ReceiptHandle")

	// Retrieve FormValues required
	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())
	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	log.Println("Deleting Message, Queue:", queueName, ", ReceiptHandle:", receiptHandle)

	// Find queue/message with the receipt handle and delete
	app.SyncQueues.Lock()
	if _, ok := app.SyncQueues.Queues[queueName]; ok {
		for i, msg := range app.SyncQueues.Queues[queueName].Messages {
			if msg.ReceiptHandle == receiptHandle {
				// Unlock messages for the group
				log.Printf("FIFO Queue %s unlocking group %s:", queueName, msg.GroupID)
				app.SyncQueues.Queues[queueName].UnlockGroup(msg.GroupID)
				//Delete message from Q
				app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages[:i], app.SyncQueues.Queues[queueName].Messages[i+1:]...)
				delete(app.SyncQueues.Queues[queueName].Duplicates, msg.DeduplicationID)

				app.SyncQueues.Unlock()
				// Create, encode/xml and send response
				respStruct := app.DeleteMessageResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000001"}}
				enc := xml.NewEncoder(w)
				enc.Indent("  ", "    ")
				if err := enc.Encode(respStruct); err != nil {
					log.Printf("error: %v\n", err)
				}
				return
			}
		}
		log.Println("Receipt Handle not found")
	} else {
		log.Println("Queue not found")
	}
	app.SyncQueues.Unlock()

	createErrorResponse(w, req, "MessageDoesNotExist")
}

func DeleteQueue(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")

	// Retrieve FormValues required
	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())
	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	log.Println("Deleting Queue:", queueName)
	app.SyncQueues.Lock()
	delete(app.SyncQueues.Queues, queueName)
	app.SyncQueues.Unlock()

	// Create, encode/xml and send response
	respStruct := app.DeleteQueueResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}

func PurgeQueue(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")

	// Retrieve FormValues required
	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())

	uriSegments := strings.Split(queueUrl, "/")
	queueName := uriSegments[len(uriSegments)-1]

	log.Println("Purging Queue:", queueName)

	app.SyncQueues.Lock()
	if _, ok := app.SyncQueues.Queues[queueName]; ok {
		app.SyncQueues.Queues[queueName].Messages = nil
		app.SyncQueues.Queues[queueName].Duplicates = make(map[string]time.Time)
		respStruct := app.PurgeQueueResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
		enc := xml.NewEncoder(w)
		enc.Indent("  ", "    ")
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
			createErrorResponse(w, req, "GeneralError")
		}
	} else {
		log.Println("Purge Queue:", queueName, ", queue does not exist!!!")
		createErrorResponse(w, req, "QueueNotFound")
	}
	app.SyncQueues.Unlock()
}

func GetQueueUrl(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")
	//
	//// Retrieve FormValues required
	queueName := req.FormValue("QueueName")
	if queue, ok := app.SyncQueues.Queues[queueName]; ok {
		url := queue.URL
		log.Println("Get Queue URL:", queueName)
		// Create, encode/xml and send response
		result := app.GetQueueUrlResult{QueueUrl: url}
		respStruct := app.GetQueueUrlResponse{"http://queue.amazonaws.com/doc/2012-11-05/", result, app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
		enc := xml.NewEncoder(w)
		enc.Indent("  ", "    ")
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
		}
	} else {
		log.Println("Get Queue URL:", queueName, ", queue does not exist!!!")
		createErrorResponse(w, req, "QueueNotFound")
	}
}

func GetQueueAttributes(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")
	// Retrieve FormValues required
	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())

	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	log.Println("Get Queue Attributes:", queueName)
	app.SyncQueues.RLock()
	if queue, ok := app.SyncQueues.Queues[queueName]; ok {
		// Create, encode/xml and send response
		attribs := make([]app.Attribute, 0, 0)
		attr := app.Attribute{Name: "VisibilityTimeout", Value: strconv.Itoa(queue.TimeoutSecs)}
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "DelaySeconds", Value: strconv.Itoa(queue.DelaySecs)}
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "ReceiveMessageWaitTimeSeconds", Value: strconv.Itoa(queue.ReceiveWaitTimeSecs)}
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "ApproximateNumberOfMessages", Value: strconv.Itoa(len(queue.Messages))}
		attribs = append(attribs, attr)
		app.SyncQueues.RLock()
		attr = app.Attribute{Name: "ApproximateNumberOfMessagesNotVisible", Value: strconv.Itoa(numberOfHiddenMessagesInQueue(*queue))}
		app.SyncQueues.RUnlock()
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "CreatedTimestamp", Value: "0000000000"}
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "LastModifiedTimestamp", Value: "0000000000"}
		attribs = append(attribs, attr)
		attr = app.Attribute{Name: "QueueArn", Value: queue.Arn}
		attribs = append(attribs, attr)

		deadLetterTargetArn := ""
		if queue.DeadLetterQueue != nil {
			deadLetterTargetArn = queue.DeadLetterQueue.Name
		}
		attr = app.Attribute{Name: "RedrivePolicy", Value: fmt.Sprintf(`{"maxReceiveCount": "%d", "deadLetterTargetArn":"%s"}`, queue.MaxReceiveCount, deadLetterTargetArn)}
		attribs = append(attribs, attr)

		result := app.GetQueueAttributesResult{Attrs: attribs}
		respStruct := app.GetQueueAttributesResponse{"http://queue.amazonaws.com/doc/2012-11-05/", result, app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
		enc := xml.NewEncoder(w)
		enc.Indent("  ", "    ")
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
		}
	} else {
		log.Println("Get Queue URL:", queueName, ", queue does not exist!!!")
		createErrorResponse(w, req, "QueueNotFound")
	}
	app.SyncQueues.RUnlock()
}

func SetQueueAttributes(w http.ResponseWriter, req *http.Request) {
	// Sent response type
	w.Header().Set("Content-Type", "application/xml")

	queueUrl := getQueueFromPath(req.FormValue("QueueUrl"), req.URL.String())

	queueName := ""
	if queueUrl == "" {
		vars := mux.Vars(req)
		queueName = vars["queueName"]
	} else {
		uriSegments := strings.Split(queueUrl, "/")
		queueName = uriSegments[len(uriSegments)-1]
	}

	log.Println("Set Queue Attributes:", queueName)
	app.SyncQueues.Lock()
	if queue, ok := app.SyncQueues.Queues[queueName]; ok {
		if err := validateAndSetQueueAttributes(queue, req.Form); err != nil {
			createErrorResponse(w, req, err.Error())
			app.SyncQueues.Unlock()
			return
		}

		respStruct := app.SetQueueAttributesResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: "00000000-0000-0000-0000-000000000000"}}
		enc := xml.NewEncoder(w)
		enc.Indent("  ", "    ")
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
		}
	} else {
		log.Println("Get Queue URL:", queueName, ", queue does not exist!!!")
		createErrorResponse(w, req, "QueueNotFound")
	}
	app.SyncQueues.Unlock()
}

func getMessageResult(m *app.Message) *app.ResultMessage {
	msgMttrs := []*app.ResultMessageAttribute{}
	for _, attr := range m.MessageAttributes {
		msgMttrs = append(msgMttrs, getMessageAttributeResult(&attr))
	}

	attrsMap := map[string]string{
		"ApproximateFirstReceiveTimestamp": fmt.Sprintf("%d", m.ReceiptTime.UnixNano()/int64(time.Millisecond)),
		"SenderId":                         app.CurrentEnvironment.AccountID,
		"ApproximateReceiveCount":          fmt.Sprintf("%d", m.NumberOfReceives+1),
		"SentTimestamp":                    fmt.Sprintf("%d", time.Now().UTC().UnixNano()/int64(time.Millisecond)),
	}

	var attrs []*app.ResultAttribute
	for k, v := range attrsMap {
		attrs = append(attrs, &app.ResultAttribute{
			Name:  k,
			Value: v,
		})
	}

	return &app.ResultMessage{
		MessageId:              m.Uuid,
		Body:                   m.MessageBody,
		ReceiptHandle:          m.ReceiptHandle,
		MD5OfBody:              common.GetMD5Hash(string(m.MessageBody)),
		MD5OfMessageAttributes: m.MD5OfMessageAttributes,
		MessageAttributes:      msgMttrs,
		Attributes:             attrs,
	}
}

func getQueueFromPath(formVal string, theUrl string) string {
	if formVal != "" {
		return formVal
	}
	u, err := url.Parse(theUrl)
	if err != nil {
		return ""
	}
	return u.Path
}

func createErrorResponse(w http.ResponseWriter, req *http.Request, err string) {
	er := app.SqsErrors[err]
	respStruct := app.ErrorResponse{
		Result:    app.ErrorResult{Type: er.Type, Code: er.Code, Message: er.Message},
		RequestId: "00000000-0000-0000-0000-000000000000",
	}

	w.WriteHeader(er.HttpError)
	enc := xml.NewEncoder(w)
	enc.Indent("  ", "    ")
	if err := enc.Encode(respStruct); err != nil {
		log.Printf("error: %v\n", err)
	}
}
