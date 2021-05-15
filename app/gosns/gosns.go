package gosns

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"io/ioutil"
	"math/big"

	"github.com/p4tin/goaws/app"
	"github.com/p4tin/goaws/app/common"
	log "github.com/sirupsen/logrus"
)

type pendingConfirm struct {
	subArn string
	token  string
}

var PemKEY []byte
var PrivateKEY *rsa.PrivateKey
var TOPIC_DATA map[string]*pendingConfirm

func init() {
	app.SyncTopics.Topics = make(map[string]*app.Topic)
	TOPIC_DATA = make(map[string]*pendingConfirm)

	app.SnsErrors = make(map[string]app.SnsErrorType)
	err1 := app.SnsErrorType{HttpError: http.StatusBadRequest, Type: "Not Found", Code: "AWS.SimpleNotificationService.NonExistentTopic", Message: "The specified topic does not exist for this wsdl version."}
	app.SnsErrors["TopicNotFound"] = err1
	err2 := app.SnsErrorType{HttpError: http.StatusBadRequest, Type: "Not Found", Code: "AWS.SimpleNotificationService.NonExistentSubscription", Message: "The specified subscription does not exist for this wsdl version."}
	app.SnsErrors["SubscriptionNotFound"] = err2
	err3 := app.SnsErrorType{HttpError: http.StatusBadRequest, Type: "Duplicate", Code: "AWS.SimpleNotificationService.TopicAlreadyExists", Message: "The specified topic already exists."}
	app.SnsErrors["TopicExists"] = err3
	err4 := app.SnsErrorType{HttpError: http.StatusBadRequest, Type: "InvalidParameter", Code: "AWS.SimpleNotificationService.ValidationError", Message: "The input fails to satisfy the constraints specified by an AWS service."}
	app.SnsErrors["ValidationError"] = err4
	PrivateKEY, PemKEY, _ = createPemFile()
}

func createPemFile() (privkey *rsa.PrivateKey, pemkey []byte, err error) {
	template := &x509.Certificate{
		IsCA:                  true,
		BasicConstraintsValid: true,
		SubjectKeyId:          []byte{11, 22, 33},
		SerialNumber:          big.NewInt(1111),
		Subject: pkix.Name{
			Country:      []string{"USA"},
			Organization: []string{"Amazon"},
		},
		NotBefore:   time.Now(),
		NotAfter:    time.Now().Add(time.Duration(5) * time.Second),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}

	// generate private key
	privkey, err = rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return
	}

	// create a self-signed certificate
	parent := template
	cert, err := x509.CreateCertificate(rand.Reader, template, parent, &privkey.PublicKey, privkey)
	if err != nil {
		return
	}

	pemkey = pem.EncodeToMemory(
		&pem.Block{
			Type:  "CERTIFICATE",
			Bytes: cert,
		},
	)
	return
}

func ListTopics(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")

	respStruct := app.ListTopicsResponse{}
	respStruct.Xmlns = "http://queue.amazonaws.com/doc/2012-11-05/"
	uuid, _ := common.NewUUID()
	respStruct.Metadata = app.ResponseMetadata{RequestId: uuid}

	respStruct.Result.Topics.Member = make([]app.TopicArnResult, 0, 0)
	log.Println("Listing Topics")
	for _, topic := range app.SyncTopics.Topics {
		ta := app.TopicArnResult{TopicArn: topic.Arn}
		respStruct.Result.Topics.Member = append(respStruct.Result.Topics.Member, ta)
	}

	SendResponseBack(w, req, respStruct, content)
}

func CreateTopic(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	topicName := req.FormValue("Name")
	topicArn := ""
	if _, ok := app.SyncTopics.Topics[topicName]; ok {
		topicArn = app.SyncTopics.Topics[topicName].Arn
	} else {
		topicArn = "arn:aws:sns:" + app.CurrentEnvironment.Region + ":" + app.CurrentEnvironment.AccountID + ":" + topicName

		log.Println("Creating Topic:", topicName)
		topic := &app.Topic{Name: topicName, Arn: topicArn}
		topic.Subscriptions = make([]*app.Subscription, 0, 0)
		app.SyncTopics.Lock()
		app.SyncTopics.Topics[topicName] = topic
		app.SyncTopics.Unlock()
	}
	uuid, _ := common.NewUUID()
	respStruct := app.CreateTopicResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.CreateTopicResult{TopicArn: topicArn}, app.ResponseMetadata{RequestId: uuid}}
	SendResponseBack(w, req, respStruct, content)
}

// aws --endpoint-url http://localhost:47194 sns subscribe --topic-arn arn:aws:sns:us-west-2:0123456789012:my-topic --protocol email --notification-endpoint my-email@example.com
func Subscribe(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	topicArn := req.FormValue("TopicArn")
	protocol := req.FormValue("Protocol")
	endpoint := req.FormValue("Endpoint")
	filterPolicy := &app.FilterPolicy{}
	raw := false

	for attrIndex := 1; req.FormValue("Attributes.entry."+strconv.Itoa(attrIndex)+".key") != ""; attrIndex++ {
		value := req.FormValue("Attributes.entry." + strconv.Itoa(attrIndex) + ".value")
		switch key := req.FormValue("Attributes.entry." + strconv.Itoa(attrIndex) + ".key"); key {
		case "FilterPolicy":
			json.Unmarshal([]byte(value), filterPolicy)
		case "RawMessageDelivery":
			raw = (value == "true")
		}
	}

	uriSegments := strings.Split(topicArn, ":")
	topicName := uriSegments[len(uriSegments)-1]
	log.WithFields(log.Fields{
		"content":      content,
		"topicArn":     topicArn,
		"topicName":    topicName,
		"protocol":     protocol,
		"endpoint":     endpoint,
		"filterPolicy": filterPolicy,
		"raw":          raw,
	}).Info("Creating Subscription")

	subscription := &app.Subscription{EndPoint: endpoint, Protocol: protocol, TopicArn: topicArn, Raw: raw, FilterPolicy: filterPolicy}
	subArn, _ := common.NewUUID()
	subArn = topicArn + ":" + subArn
	subscription.SubscriptionArn = subArn

	if app.SyncTopics.Topics[topicName] != nil {
		app.SyncTopics.Lock()
		isDuplicate := false
		// Duplicate check
		for _, subscription := range app.SyncTopics.Topics[topicName].Subscriptions {
			if subscription.EndPoint == endpoint && subscription.TopicArn == topicArn {
				isDuplicate = true
				subArn = subscription.SubscriptionArn
			}
		}
		if !isDuplicate {
			app.SyncTopics.Topics[topicName].Subscriptions = append(app.SyncTopics.Topics[topicName].Subscriptions, subscription)
			log.WithFields(log.Fields{
				"topic":    topicName,
				"endpoint": endpoint,
				"topicArn": topicArn,
			}).Debug("Created subscription")
		}
		app.SyncTopics.Unlock()

		//Create the response
		uuid, _ := common.NewUUID()
		if app.Protocol(subscription.Protocol) == app.ProtocolHTTP || app.Protocol(subscription.Protocol) == app.ProtocolHTTPS {
			id, _ := common.NewUUID()
			token, _ := common.NewUUID()

			TOPIC_DATA[topicArn] = &pendingConfirm{
				subArn: subArn,
				token:  token,
			}

			respStruct := app.SubscribeResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.SubscribeResult{SubscriptionArn: subArn}, app.ResponseMetadata{RequestId: uuid}}
			SendResponseBack(w, req, respStruct, content)
			time.Sleep(time.Second)

			snsMSG := &app.SNSMessage{
				Type:             "SubscriptionConfirmation",
				MessageId:        id,
				Token:            token,
				TopicArn:         topicArn,
				Message:          "You have chosen to subscribe to the topic " + topicArn + ".\nTo confirm the subscription, visit the SubscribeURL included in this message.",
				SigningCertURL:   "http://" + req.Host + "/SimpleNotificationService/" + uuid + ".pem",
				SignatureVersion: "1",
				SubscribeURL:     "http://" + req.Host + "/?Action=ConfirmSubscription&TopicArn=" + topicArn + "&Token=" + token,
				Timestamp:        time.Now().UTC().Format(time.RFC3339),
			}
			signature, err := signMessage(PrivateKEY, snsMSG)
			if err != nil {
				log.Error("Error signing message")
			} else {
				snsMSG.Signature = signature
			}
			err = callEndpoint(subscription.EndPoint, uuid, *snsMSG, subscription.Raw)
			if err != nil {
				log.Error("Error posting to url ", err)
			}
		} else {
			respStruct := app.SubscribeResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.SubscribeResult{SubscriptionArn: subArn}, app.ResponseMetadata{RequestId: uuid}}

			SendResponseBack(w, req, respStruct, content)
		}

	} else {
		createErrorResponse(w, req, "TopicNotFound")
	}
}

func signMessage(privkey *rsa.PrivateKey, snsMsg *app.SNSMessage) (string, error) {
	fs, err := formatSignature(snsMsg)
	if err != nil {
		return "", nil
	}

	h := sha1.Sum([]byte(fs))
	signature_b, err := rsa.SignPKCS1v15(rand.Reader, privkey, crypto.SHA1, h[:])

	return base64.StdEncoding.EncodeToString(signature_b), err
}

func formatSignature(msg *app.SNSMessage) (formated string, err error) {
	if msg.Type == "Notification" && msg.Subject != "" {
		formated = fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
			"Message", msg.Message,
			"MessageId", msg.MessageId,
			"Subject", msg.Subject,
			"Timestamp", msg.Timestamp,
			"TopicArn", msg.TopicArn,
			"Type", msg.Type,
		)
	} else if msg.Type == "Notification" && msg.Subject == "" {
		formated = fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
			"Message", msg.Message,
			"MessageId", msg.MessageId,
			"Timestamp", msg.Timestamp,
			"TopicArn", msg.TopicArn,
			"Type", msg.Type,
		)
	} else if msg.Type == "SubscriptionConfirmation" || msg.Type == "UnsubscribeConfirmation" {
		formated = fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n",
			"Message", msg.Message,
			"MessageId", msg.MessageId,
			"SubscribeURL", msg.SubscribeURL,
			"Timestamp", msg.Timestamp,
			"Token", msg.Token,
			"TopicArn", msg.TopicArn,
			"Type", msg.Type,
		)
	} else {
		return formated, errors.New("Unable to determine SNSMessage type")
	}

	return
}

func ConfirmSubscription(w http.ResponseWriter, req *http.Request) {
	topicArn := req.Form.Get("TopicArn")
	confirmToken := req.Form.Get("Token")
	pendingConfirm := TOPIC_DATA[topicArn]
	if pendingConfirm.token == confirmToken {
		uuid, _ := common.NewUUID()
		respStruct := app.ConfirmSubscriptionResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.SubscribeResult{SubscriptionArn: pendingConfirm.subArn}, app.ResponseMetadata{RequestId: uuid}}

		SendResponseBack(w, req, respStruct, "application/xml")
	} else {
		createErrorResponse(w, req, "SubArnNotFound")
	}

}

func ListSubscriptions(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")

	uuid, _ := common.NewUUID()
	respStruct := app.ListSubscriptionsResponse{}
	respStruct.Xmlns = "http://queue.amazonaws.com/doc/2012-11-05/"
	respStruct.Metadata.RequestId = uuid
	respStruct.Result.Subscriptions.Member = make([]app.TopicMemberResult, 0, 0)

	for _, topic := range app.SyncTopics.Topics {
		for _, sub := range topic.Subscriptions {
			tar := app.TopicMemberResult{TopicArn: topic.Arn, Protocol: sub.Protocol,
				SubscriptionArn: sub.SubscriptionArn, Endpoint: sub.EndPoint}
			respStruct.Result.Subscriptions.Member = append(respStruct.Result.Subscriptions.Member, tar)
		}
	}

	SendResponseBack(w, req, respStruct, content)
}

func ListSubscriptionsByTopic(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	topicArn := req.FormValue("TopicArn")

	uriSegments := strings.Split(topicArn, ":")
	topicName := uriSegments[len(uriSegments)-1]

	if topic, ok := app.SyncTopics.Topics[topicName]; ok {
		uuid, _ := common.NewUUID()
		respStruct := app.ListSubscriptionsByTopicResponse{}
		respStruct.Xmlns = "http://queue.amazonaws.com/doc/2012-11-05/"
		respStruct.Metadata.RequestId = uuid
		respStruct.Result.Subscriptions.Member = make([]app.TopicMemberResult, 0, 0)

		for _, sub := range topic.Subscriptions {
			tar := app.TopicMemberResult{TopicArn: topic.Arn, Protocol: sub.Protocol,
				SubscriptionArn: sub.SubscriptionArn, Endpoint: sub.EndPoint}
			respStruct.Result.Subscriptions.Member = append(respStruct.Result.Subscriptions.Member, tar)
		}
		SendResponseBack(w, req, respStruct, content)
	} else {
		createErrorResponse(w, req, "TopicNotFound")
	}
}

func SetSubscriptionAttributes(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	subsArn := req.FormValue("SubscriptionArn")
	Attribute := req.FormValue("AttributeName")
	Value := req.FormValue("AttributeValue")

	for _, topic := range app.SyncTopics.Topics {
		for _, sub := range topic.Subscriptions {
			if sub.SubscriptionArn == subsArn {
				if Attribute == "RawMessageDelivery" {
					app.SyncTopics.Lock()
					if Value == "true" {
						sub.Raw = true
					} else {
						sub.Raw = false
					}
					app.SyncTopics.Unlock()
					//Good Response == return
					uuid, _ := common.NewUUID()
					respStruct := app.SetSubscriptionAttributesResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: uuid}}
					SendResponseBack(w, req, respStruct, content)
					return
				}

				if Attribute == "FilterPolicy" {
					filterPolicy := &app.FilterPolicy{}
					err := json.Unmarshal([]byte(Value), filterPolicy)
					if err != nil {
						createErrorResponse(w, req, "ValidationError")
						return
					}

					app.SyncTopics.Lock()
					sub.FilterPolicy = filterPolicy
					app.SyncTopics.Unlock()

					//Good Response == return
					uuid, _ := common.NewUUID()
					respStruct := app.SetSubscriptionAttributesResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: uuid}}
					SendResponseBack(w, req, respStruct, content)
					return
				}

			}
		}
	}
	createErrorResponse(w, req, "SubscriptionNotFound")
}

func GetSubscriptionAttributes(w http.ResponseWriter, req *http.Request) {

	content := req.FormValue("ContentType")
	subsArn := req.FormValue("SubscriptionArn")

	for _, topic := range app.SyncTopics.Topics {
		for _, sub := range topic.Subscriptions {
			if sub.SubscriptionArn == subsArn {

				entries := make([]app.SubscriptionAttributeEntry, 0, 0)
				entry := app.SubscriptionAttributeEntry{Key: "Owner", Value: app.CurrentEnvironment.AccountID}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "RawMessageDelivery", Value: strconv.FormatBool(sub.Raw)}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "TopicArn", Value: sub.TopicArn}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "Endpoint", Value: sub.EndPoint}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "PendingConfirmation", Value: "false"}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "ConfirmationWasAuthenticated", Value: "true"}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "SubscriptionArn", Value: sub.SubscriptionArn}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "Protocol", Value: sub.Protocol}
				entries = append(entries, entry)
				entry = app.SubscriptionAttributeEntry{Key: "Endpoint", Value: sub.EndPoint}
				entries = append(entries, entry)

				if sub.FilterPolicy != nil {
					filterPolicyBytes, _ := json.Marshal(sub.FilterPolicy)
					entry = app.SubscriptionAttributeEntry{Key: "FilterPolicy", Value: string(filterPolicyBytes)}
					entries = append(entries, entry)
				}

				result := app.GetSubscriptionAttributesResult{SubscriptionAttributes: app.SubscriptionAttributes{Entries: entries}}
				uuid, _ := common.NewUUID()
				respStruct := app.GetSubscriptionAttributesResponse{"http://sns.amazonaws.com/doc/2010-03-31", result, app.ResponseMetadata{RequestId: uuid}}

				SendResponseBack(w, req, respStruct, content)

				return
			}
		}
	}
	createErrorResponse(w, req, "SubscriptionNotFound")
}

func Unsubscribe(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	subArn := req.FormValue("SubscriptionArn")

	log.Println("Unsubscribe:", subArn)
	for _, topic := range app.SyncTopics.Topics {
		for i, sub := range topic.Subscriptions {
			if sub.SubscriptionArn == subArn {
				app.SyncTopics.Lock()

				copy(topic.Subscriptions[i:], topic.Subscriptions[i+1:])
				topic.Subscriptions[len(topic.Subscriptions)-1] = nil
				topic.Subscriptions = topic.Subscriptions[:len(topic.Subscriptions)-1]

				app.SyncTopics.Unlock()

				uuid, _ := common.NewUUID()
				respStruct := app.UnsubscribeResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: uuid}}
				SendResponseBack(w, req, respStruct, content)
				return
			}
		}
	}
	createErrorResponse(w, req, "SubscriptionNotFound")
}

func DeleteTopic(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	topicArn := req.FormValue("TopicArn")

	uriSegments := strings.Split(topicArn, ":")
	topicName := uriSegments[len(uriSegments)-1]

	log.Println("Delete Topic - TopicName:", topicName)

	_, ok := app.SyncTopics.Topics[topicName]
	if ok {
		app.SyncTopics.Lock()
		delete(app.SyncTopics.Topics, topicName)
		app.SyncTopics.Unlock()
		uuid, _ := common.NewUUID()
		respStruct := app.DeleteTopicResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.ResponseMetadata{RequestId: uuid}}
		SendResponseBack(w, req, respStruct, content)
	} else {
		createErrorResponse(w, req, "TopicNotFound")
	}

}

// aws --endpoint-url http://localhost:47194 sns publish --topic-arn arn:aws:sns:yopa-local:000000000000:test1 --message "This is a test"
func Publish(w http.ResponseWriter, req *http.Request) {
	content := req.FormValue("ContentType")
	topicArn := req.FormValue("TopicArn")
	subject := req.FormValue("Subject")
	messageBody := req.FormValue("Message")
	messageStructure := req.FormValue("MessageStructure")
	messageAttributes := getMessageAttributesFromRequest(req)

	arnSegments := strings.Split(topicArn, ":")
	topicName := arnSegments[len(arnSegments)-1]

	_, ok := app.SyncTopics.Topics[topicName]
	if ok {
		log.WithFields(log.Fields{
			"topic":    topicName,
			"topicArn": topicArn,
			"subject":  subject,
		}).Debug("Publish to Topic")
		for _, subs := range app.SyncTopics.Topics[topicName].Subscriptions {
			switch app.Protocol(subs.Protocol) {
			case app.ProtocolSQS:
				publishSQS(w, req, subs, messageBody, messageAttributes, subject, topicArn, topicName, messageStructure)
			case app.ProtocolHTTP:
				fallthrough
			case app.ProtocolHTTPS:
				publishHTTP(req.Host, subs, messageBody, messageAttributes, subject, topicArn)
			}
		}
	} else {
		createErrorResponse(w, req, "TopicNotFound")
		return
	}

	//Create the response
	msgId, _ := common.NewUUID()
	uuid, _ := common.NewUUID()
	respStruct := app.PublishResponse{"http://queue.amazonaws.com/doc/2012-11-05/", app.PublishResult{MessageId: msgId}, app.ResponseMetadata{RequestId: uuid}}
	SendResponseBack(w, req, respStruct, content)
}

func publishSQS(w http.ResponseWriter, req *http.Request,
	subs *app.Subscription, messageBody string, messageAttributes map[string]app.MessageAttributeValue,
	subject string, topicArn string, topicName string, messageStructure string) {
	if subs.FilterPolicy != nil && !subs.FilterPolicy.IsSatisfiedBy(messageAttributes) {
		return
	}

	endPoint := subs.EndPoint
	uriSegments := strings.Split(endPoint, "/")
	queueName := uriSegments[len(uriSegments)-1]
	arnSegments := strings.Split(queueName, ":")
	queueName = arnSegments[len(arnSegments)-1]

	if _, ok := app.SyncQueues.Queues[queueName]; ok {
		msg := app.Message{}

		if subs.Raw == false {
			m, err := CreateMessageBody(req.Host, subs, messageBody, subject, messageStructure, messageAttributes)
			if err != nil {
				createErrorResponse(w, req, err.Error())
				return
			}

			msg.MessageBody = m
		} else {
			msg.MessageAttributes = messageAttributes
			msg.MD5OfMessageAttributes = common.HashAttributes(messageAttributes)
			m, err := extractMessageFromJSON(messageBody, subs.Protocol)
			if (err == nil) {
				msg.MessageBody = []byte(m)
			} else {
				msg.MessageBody = []byte(messageBody)
			}
		}

		msg.MD5OfMessageBody = common.GetMD5Hash(messageBody)
		msg.Uuid, _ = common.NewUUID()
		app.SyncQueues.Lock()
		app.SyncQueues.Queues[queueName].Messages = append(app.SyncQueues.Queues[queueName].Messages, msg)
		app.SyncQueues.Unlock()

		log.Infof("%s: Topic: %s(%s), Message: %s\n", time.Now().Format("2006-01-02 15:04:05"), topicName, queueName, msg.MessageBody)
	} else {
		log.Infof("%s: Queue %s does not exist, message discarded\n", time.Now().Format("2006-01-02 15:04:05"), queueName)
	}
}

func publishHTTP(host string, subs *app.Subscription, messageBody string, messageAttributes map[string]app.MessageAttributeValue,
	subject string, topicArn string) {
	id, _ := common.NewUUID()
	msg := app.SNSMessage{
		Type:              "Notification",
		MessageId:         id,
		TopicArn:          topicArn,
		Subject:           subject,
		Message:           messageBody,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		SignatureVersion:  "1",
		SigningCertURL:    "http://" + host + "/SimpleNotificationService/" + id + ".pem",
		UnsubscribeURL:    "http://" + host + "/?Action=Unsubscribe&SubscriptionArn=" + subs.SubscriptionArn,
		MessageAttributes: formatAttributes(messageAttributes),
	}

	signature, err := signMessage(PrivateKEY, &msg)
	if err != nil {
		log.Error(err)
	} else {
		msg.Signature = signature
	}
	err = callEndpoint(subs.EndPoint, subs.SubscriptionArn, msg, subs.Raw)
	if err != nil {
		log.WithFields(log.Fields{
			"EndPoint": subs.EndPoint,
			"ARN":      subs.SubscriptionArn,
			"error":    err.Error(),
		}).Error("Error calling endpoint")
	}
}

func formatAttributes(values map[string]app.MessageAttributeValue) map[string]app.MsgAttr {
	attr := make(map[string]app.MsgAttr)
	for k, v := range values {
		attr[k] = app.MsgAttr{
			Type:  v.ValueKey,
			Value: v.Value,
		}
	}
	return attr
}

func callEndpoint(endpoint string, subArn string, msg app.SNSMessage, raw bool) error {
	log.WithFields(log.Fields{
		"sns":      msg,
		"subArn":   subArn,
		"endpoint": endpoint,
	}).Debug("Calling endpoint")
	var err error
	var byteData []byte

	if raw {
		byteData, err = json.Marshal(msg.Message)
	} else {
		byteData, err = json.Marshal(msg)
	}
	if err != nil {
		return err
	}
	req, err := http.NewRequest("POST", endpoint, bytes.NewReader(byteData))
	if err != nil {
		return err
	}

	//req.Header.Add("Authorization", "Basic YXV0aEhlYWRlcg==")
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("x-amz-sns-message-type", msg.Type)
	req.Header.Add("x-amz-sns-message-id", msg.MessageId)
	req.Header.Add("x-amz-sns-topic-arn", msg.TopicArn)
	req.Header.Add("x-amz-sns-subscription-arn", subArn)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	if res == nil {
		return errors.New("response is nil")
	}
	if res.StatusCode%200 != 0 {
		log.WithFields(log.Fields{
			"statusCode": res.StatusCode,
			"status":     res.Status,
			"header":     res.Header,
			"endpoint":   endpoint,
		}).Error("Not 2xx repsone")
		return errors.New("Not 2xx response")
	}

	defer res.Body.Close()
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}

	log.WithFields(log.Fields{
		"body": string(body),
		"res":  res,
	}).Debug("Received successful response")

	return nil
}

func getMessageAttributesFromRequest(req *http.Request) map[string]app.MessageAttributeValue {
	attributes := make(map[string]app.MessageAttributeValue)

	for i := 1; true; i++ {
		name := req.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Name", i))
		if name == "" {
			break
		}

		dataType := req.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Value.DataType", i))
		if dataType == "" {
			log.Warnf("DataType of MessageAttribute %s is missing, MD5 checksum will most probably be wrong!\n", name)
			continue
		}

		// StringListValue and BinaryListValue is currently not implemented
		for _, valueKey := range [...]string{"StringValue", "BinaryValue"} {
			value := req.FormValue(fmt.Sprintf("MessageAttributes.entry.%d.Value.%s", i, valueKey))
			if value != "" {
				attributes[name] = app.MessageAttributeValue{name, dataType, value, valueKey}
			}
		}

		if _, ok := attributes[name]; !ok {
			log.Warnf("StringValue or BinaryValue of MessageAttribute %s is missing, MD5 checksum will most probably be wrong!\n", name)
		}
	}

	return attributes
}

func CreateMessageBody(host string, subs *app.Subscription, msg string, subject string, messageStructure string,
	messageAttributes map[string]app.MessageAttributeValue) ([]byte, error) {

	msgId, _ := common.NewUUID()

	message := app.SNSMessage{
		Type:              "Notification",
		MessageId:         msgId,
		TopicArn:          subs.TopicArn,
		Subject:           subject,
		Timestamp:         time.Now().UTC().Format(time.RFC3339),
		SignatureVersion:  "1",
		SigningCertURL:    "http://" + host + "/SimpleNotificationService/" + msgId + ".pem",
		UnsubscribeURL:    "http://" + host + "/?Action=Unsubscribe&SubscriptionArn=" + subs.SubscriptionArn,
		MessageAttributes: formatAttributes(messageAttributes),
	}

	if app.MessageStructure(messageStructure) == app.MessageStructureJSON {
		m, err := extractMessageFromJSON(msg, subs.Protocol)
		if err != nil {
			return nil, err
		}
		message.Message = m
	} else {
		message.Message = msg
	}

	signature, err := signMessage(PrivateKEY, &message)
	if err != nil {
		log.Error(err)
	} else {
		message.Signature = signature
	}

	byteMsg, _ := json.Marshal(message)
	return byteMsg, nil
}

func extractMessageFromJSON(msg string, protocol string) (string, error) {
	var msgWithProtocols map[string]string
	if err := json.Unmarshal([]byte(msg), &msgWithProtocols); err != nil {
		return "", err
	}

	defaultMsg, ok := msgWithProtocols[string(app.ProtocolDefault)]
	if !ok {
		return "", errors.New(app.ErrNoDefaultElementInJSON)
	}

	if m, ok := msgWithProtocols[protocol]; ok {
		return m, nil
	}

	return defaultMsg, nil
}

func createErrorResponse(w http.ResponseWriter, req *http.Request, err string) {
	er := app.SnsErrors[err]
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

func SendResponseBack(w http.ResponseWriter, req *http.Request, respStruct interface{}, content string) {
	if content == "JSON" {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
		}
	} else {
		w.Header().Set("Content-Type", "application/xml")
		enc := xml.NewEncoder(w)
		enc.Indent("  ", "    ")
		if err := enc.Encode(respStruct); err != nil {
			log.Printf("error: %v\n", err)
		}
	}
}
