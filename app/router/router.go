package router

import (
	"io"
	"net/http"

	log "github.com/sirupsen/logrus"

	"fmt"

	"github.com/gorilla/mux"
	app "github.com/p4tin/goaws/app"
	sns "github.com/p4tin/goaws/app/gosns"
	sqs "github.com/p4tin/goaws/app/gosqs"
)

// New returns a new router
func New() http.Handler {
	r := mux.NewRouter()

	r.HandleFunc("/", actionHandler).Methods("GET", "POST")
	r.HandleFunc("/health", health).Methods("GET")
	r.HandleFunc("/{account}", actionHandler).Methods("GET", "POST")
	r.HandleFunc("/queue/{queueName}", actionHandler).Methods("GET", "POST")
	r.HandleFunc("/SimpleNotificationService/{id}.pem", pemHandler).Methods("GET")
	r.HandleFunc("/{account}/{queueName}", actionHandler).Methods("GET", "POST")

	return r
}

var routingTable = map[string]http.HandlerFunc{
	// SQS
	"ListQueues":              sqs.ListQueues,
	"CreateQueue":             sqs.CreateQueue,
	"GetQueueAttributes":      sqs.GetQueueAttributes,
	"SetQueueAttributes":      sqs.SetQueueAttributes,
	"ListQueueTags":           sqs.ListQueueTags,
	"SendMessage":             sqs.SendMessage,
	"SendMessageBatch":        sqs.SendMessageBatch,
	"ReceiveMessage":          sqs.ReceiveMessage,
	"DeleteMessage":           sqs.DeleteMessage,
	"DeleteMessageBatch":      sqs.DeleteMessageBatch,
	"GetQueueUrl":             sqs.GetQueueUrl,
	"PurgeQueue":              sqs.PurgeQueue,
	"DeleteQueue":             sqs.DeleteQueue,
	"ChangeMessageVisibility": sqs.ChangeMessageVisibility,

	// SNS
	"ListTopics":                sns.ListTopics,
	"CreateTopic":               sns.CreateTopic,
	"DeleteTopic":               sns.DeleteTopic,
	"GetTopicAttributes":        sns.GetTopicAttributes,
	"Subscribe":                 sns.Subscribe,
	"ConfirmSubscription":       sns.ConfirmSubscription,
	"SetSubscriptionAttributes": sns.SetSubscriptionAttributes,
	"GetSubscriptionAttributes": sns.GetSubscriptionAttributes,
	"ListSubscriptionsByTopic":  sns.ListSubscriptionsByTopic,
	"ListSubscriptions":         sns.ListSubscriptions,
	"ListTagsForResource":       sns.ListTagsForResource,
	"Unsubscribe":               sns.Unsubscribe,
	"Publish":                   sns.Publish,
}

func health(w http.ResponseWriter, req *http.Request) {
	//Add locks to healthcheck so we actually assert the health of things, if deadlocked it will fail
	app.SyncQueues.Lock()
	w.WriteHeader(200)
	fmt.Fprint(w, "OK")
	app.SyncQueues.Unlock()
}

func actionHandler(w http.ResponseWriter, req *http.Request) {
	log.WithFields(
		log.Fields{
			"action": req.FormValue("Action"),
			"url":    req.URL,
		}).Debug("Handling URL request")
	fn, ok := routingTable[req.FormValue("Action")]
	if !ok {
		log.Warnf("Bad Request - Action: %s", req.FormValue("Action"))
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "Bad Request")
		return
	}

	http.HandlerFunc(fn).ServeHTTP(w, req)
}

func pemHandler(w http.ResponseWriter, req *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write(sns.PemKEY)
}
