# GoAws
[![Build Status](https://travis-ci.org/p4tin/goaws.svg?branch=master)](https://travis-ci.org/p4tin/goaws)

You are always welcome to [tweet me](https://twitter.com/gocodecloud) or [buy me a coffee](https://www.paypal.me/p4tin)

Written in Go this is a clone of the AWS SQS/SNS systems.  This system is designed to emulate SQS and SNS in a local environment so developers can test their interfaces without having to connect to the AWS Cloud and possibly incurring the expense, or even worse actually write to production topics/queues by mistake.  If you see any problems or would like to see a new feature, please open an issue here in github.  As well, I will logon to Gitter so we can discuss your deployment issues or the weather.


## SNS/SQS Api status:

All SNS/SQS APIs have been implemented except:
 - The full capabilities for Get and Set QueueAttributes.  At the moment you can only Get ALL the attributes.

Here is a list of the APIs:
 - [x] ListQueues
 - [x] CreateQueue
 - [x] GetQueueAttributes (Always returns all attributes - unsupporterd arttributes are mocked)
 - [x] GetQueueUrl
 - [x] SendMessage
 - [x] SendMessageBatch
 - [x] ReceiveMessage
 - [x] DeleteMessage
 - [x] DeleteMessageBatch
 - [x] PurgeQueue
 - [x] Delete Queue
 - [x] ChangeMessageVisibility
 - [ ] ChangeMessageVisibilityBatch
 - [ ] ListDeadLetterSourceQueues
 - [ ] ListQueueTags
 - [ ] RemovePermission
 - [x] SetQueueAttributes (Only supported attributes are set - see Supported Queue Attributes)
 - [x] GetQueueAttributes (Only supported attributes are set - see Supported Queue Attributes)
 - [x] ListQueueTags (Always return empty result)
 - [ ] TagQueue
 - [ ] UntagQueue

## Supported Queue Attributes

 - [x] VisibilityTimeout
 - [x] ReceiveMessageWaitTimeSeconds
 - [x] RedrivePolicy

## Current SNS APIs implemented:

 - [x] ListTopics
 - [x] CreateTopic
 - [x] Subscribe (raw)
 - [x] ListSubscriptions
 - [x] ListTagsForResource (Always return empty result)
 - [x] Publish
 - [x] DeleteTopic
 - [x] Subscribe
 - [x] Unsubscribe
 - [X] ListSubscriptionsByTopic
 - [x] GetSubscriptionAttributes
 - [x] SetSubscriptionAttributes (Only supported attributes are set - see Supported Subscription Attributes)
 - [x] GetTopicAttributes (Only supported attributes are set - see Supported Topic Attributes)

## Supported Subscription Attributes

  - [x] RawMessageDelivery
  - [x] FilterPolicy (Only supported simplest "exact match" filter policy)

## Supported Topic Attributes

  - [x] Owner
  - [x] TopicArn


## Yaml Configuration Implemented

 - [x] Read config file
 - [x] -config flag to read a specific configuration file (e.g.: -config=myconfig.yaml)
 - [x] a command line argument to determine the environment to use in the config file (e.e.: Dev)
 - [x] IN the config file you can create Queues, Topic and Subscription see the example config file in the conf directory

## Info Logging (e.g.: -info) and Debug logging can be turned on via a command line flag (e.g.: -debug), default is Warn.

## Note:  The system does not authenticate or presently use https

# Installation

    go get github.com/p4tin/goaws/...

## Build and Run (Standalone)

    Build
        cd to GoAws directory
        go build -o goaws app/cmd/goaws.go  (The goaws executable should be in the currect directory, move it somewhere in your $PATH)

    Run
        ./goaws  (by default goaws listens on port 4100 but you can change it in the goaws.yaml file to another port of your choice)


## Run (Docker Version)

    Get it
        docker pull pafortin/goaws

    run
        docker run -d --name goaws -p 4100:4100 pafortin/goaws



## Testing your installation

You can test that your installation is working correctly in one of two ways:

 1.  Usign the postman collection, use this [link to import it](https://www.getpostman.com/collections/091386eae8c70588348e).  As well the Environment variable for the collection should be set as follows:  URL = http://localhost:4100/.

 2. by using the AWS cli tools ([download link](http://docs.aws.amazon.com/cli/latest/userguide/installing.html)) here are some samples, you can refer to the [aws cli tools docs](http://docs.aws.amazon.com/cli/latest/reference/) for further information.

* aws --endpoint-url http://localhost:4100 sqs create-queue --queue-name test1
```json
{
    "QueueUrl": "http://localhost:4100/test1"
}
```
* aws --endpoint-url http://localhost:4100 sqs list-queues
```json
{
    "QueueUrls": [
        "http://localhost:4100/test1"
    ]
}
```
* aws --endpoint-url http://localhost:4100 sqs send-message --queue-url http://localhost:4100/test1 --message-body "this is a test of the GoAws Queue messaging"
```json
{
    "MD5OfMessageBody": "9d3f5eaac3b1b4dd509f39e71e25f954",
    "MD5OfMessageAttributes": "b095c6d16871105acb75d59332513337",
    "MessageId": "66a1b4f5-cecf-473e-92b6-810156d41bbe"
}
```
* aws --endpoint-url http://localhost:4100 sqs receive-message --queue-url http://localhost:4100/test1
```json
{
    "Messages": [
        {
            "Body": "this is a test of the GoAws Queue messaging",
            "MD5OfMessageAttributes": "b095c6d16871105acb75d59332513337",
            "ReceiptHandle": "66a1b4f5-cecf-473e-92b6-810156d41bbe#f1fc455c-698e-442e-9747-f415bee5b461",
            "MD5OfBody": "9d3f5eaac3b1b4dd509f39e71e25f954",
            "MessageId": "66a1b4f5-cecf-473e-92b6-810156d41bbe"
        }
    ]
}
```
* aws --endpoint-url http://localhost:4100 sqs delete-message --queue-url http://localhost:4100/test1 --receipt-handle 66a1b4f5-cecf-473e-92b6-810156d41bbe#f1fc455c-698e-442e-9747-f415bee5b461
```
No output
```
* aws --endpoint-url http://localhost:4100 sqs receive-message --queue-url http://localhost:4100/test1
```
No output (No messages in Q)
```
* aws --endpoint-url http://localhost:4100 sqs delete-queue --queue-url http://localhost:4100/test1
```
No output
```
* aws --endpoint-url http://localhost:4100 sqs list-queues
```
No output (There are no Queues left)
```

* aws --endpoint-url http://localhost:4100 sns list-topics  (Example Response from list-topics)
```json
{
    "Topics": [
        {
            "TopicArn": "arn:aws:sns:local:000000000000:topic1"
        },
        {
            "TopicArn": "arn:aws:sns:local:000000000000:topic2"
        }
    ]
}
```
