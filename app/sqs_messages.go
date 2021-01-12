package app

/*** List Queues Response */
type ListQueuesResult struct {
	QueueUrl []string `xml:"QueueUrl"`
}

type ListQueuesResponse struct {
	Xmlns    string           `xml:"xmlns,attr"`
	Result   ListQueuesResult `xml:"ListQueuesResult"`
	Metadata ResponseMetadata `xml:"ResponseMetadata"`
}

/*** Create Queue Response */
type CreateQueueResult struct {
	QueueUrl string `xml:"QueueUrl"`
}

type CreateQueueResponse struct {
	Xmlns    string            `xml:"xmlns,attr"`
	Result   CreateQueueResult `xml:"CreateQueueResult"`
	Metadata ResponseMetadata  `xml:"ResponseMetadata"`
}

/*** Send Message Response */

type SendMessageResult struct {
	MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes"`
	MD5OfMessageBody       string `xml:"MD5OfMessageBody"`
	MessageId              string `xml:"MessageId"`
	SequenceNumber         string `xml:"SequenceNumber"`
}

type SendMessageResponse struct {
	Xmlns    string            `xml:"xmlns,attr"`
	Result   SendMessageResult `xml:"SendMessageResult"`
	Metadata ResponseMetadata  `xml:"ResponseMetadata"`
}

/*** Receive Message Response */

type ResultMessage struct {
	MessageId              string                    `xml:"MessageId,omitempty"`
	ReceiptHandle          string                    `xml:"ReceiptHandle,omitempty"`
	MD5OfBody              string                    `xml:"MD5OfBody,omitempty"`
	Body                   []byte                    `xml:"Body,omitempty"`
	MD5OfMessageAttributes string                    `xml:"MD5OfMessageAttributes,omitempty"`
	MessageAttributes      []*ResultMessageAttribute `xml:"MessageAttribute,omitempty"`
	Attributes             []*ResultAttribute        `xml:"Attribute,omitempty"`
}

type ResultMessageAttributeValue struct {
	DataType    string `xml:"DataType,omitempty"`
	StringValue string `xml:"StringValue,omitempty"`
	BinaryValue string `xml:"BinaryValue,omitempty"`
}

type ResultMessageAttribute struct {
	Name  string                       `xml:"Name,omitempty"`
	Value *ResultMessageAttributeValue `xml:"Value,omitempty"`
}

type ResultAttribute struct {
	Name  string `xml:"Name,omitempty"`
	Value string `xml:"Value,omitempty"`
}

type ReceiveMessageResult struct {
	Message []*ResultMessage `xml:"Message,omitempty"`
}

type ReceiveMessageResponse struct {
	Xmlns    string               `xml:"xmlns,attr"`
	Result   ReceiveMessageResult `xml:"ReceiveMessageResult"`
	Metadata ResponseMetadata     `xml:"ResponseMetadata"`
}

type ChangeMessageVisibilityResult struct {
	Xmlns    string           `xml:"xmlns,attr"`
	Metadata ResponseMetadata `xml:"ResponseMetadata"`
}

/*** Delete Message Response */
type DeleteMessageResponse struct {
	Xmlns    string           `xml:"xmlns,attr,omitempty"`
	Metadata ResponseMetadata `xml:"ResponseMetadata,omitempty"`
}

type DeleteQueueResponse struct {
	Xmlns    string           `xml:"xmlns,attr,omitempty"`
	Metadata ResponseMetadata `xml:"ResponseMetadata,omitempty"`
}

type DeleteMessageBatchResultEntry struct {
	Id string `xml:"Id"`
}

type SendMessageBatchResultEntry struct {
	Id                     string `xml:"Id"`
	MessageId              string `xml:"MessageId"`
	MD5OfMessageBody       string `xml:"MD5OfMessageBody,omitempty"`
	MD5OfMessageAttributes string `xml:"MD5OfMessageAttributes,omitempty"`
	SequenceNumber         string `xml:"SequenceNumber"`
}

type BatchResultErrorEntry struct {
	Code        string `xml:"Code"`
	Id          string `xml:"Id"`
	Message     string `xml:"Message,omitempty"`
	SenderFault bool   `xml:"SenderFault"`
}

type DeleteMessageBatchResult struct {
	Entry []DeleteMessageBatchResultEntry `xml:"DeleteMessageBatchResultEntry"`
	Error []BatchResultErrorEntry         `xml:"BatchResultErrorEntry,omitempty"`
}

/*** Delete Message Batch Response */
type DeleteMessageBatchResponse struct {
	Xmlns    string                   `xml:"xmlns,attr,omitempty"`
	Result   DeleteMessageBatchResult `xml:"DeleteMessageBatchResult"`
	Metadata ResponseMetadata         `xml:"ResponseMetadata,omitempty"`
}

type SendMessageBatchResult struct {
	Entry []SendMessageBatchResultEntry `xml:"SendMessageBatchResultEntry"`
	Error []BatchResultErrorEntry       `xml:"BatchResultErrorEntry,omitempty"`
}

/*** Delete Message Batch Response */
type SendMessageBatchResponse struct {
	Xmlns    string                 `xml:"xmlns,attr,omitempty"`
	Result   SendMessageBatchResult `xml:"SendMessageBatchResult"`
	Metadata ResponseMetadata       `xml:"ResponseMetadata,omitempty"`
}

/*** Purge Queue Response */
type PurgeQueueResponse struct {
	Xmlns    string           `xml:"xmlns,attr,omitempty"`
	Metadata ResponseMetadata `xml:"ResponseMetadata,omitempty"`
}

/*** Get Queue Url Response */
type GetQueueUrlResult struct {
	QueueUrl string `xml:"QueueUrl,omitempty"`
}

type GetQueueUrlResponse struct {
	Xmlns    string            `xml:"xmlns,attr,omitempty"`
	Result   GetQueueUrlResult `xml:"GetQueueUrlResult"`
	Metadata ResponseMetadata  `xml:"ResponseMetadata,omitempty"`
}

/*** Get Queue Attributes ***/
type Attribute struct {
	Name  string `xml:"Name,omitempty"`
	Value string `xml:"Value,omitempty"`
}

type GetQueueAttributesResult struct {
	/* VisibilityTimeout, DelaySeconds, ReceiveMessageWaitTimeSeconds, ApproximateNumberOfMessages
	   ApproximateNumberOfMessagesNotVisible, CreatedTimestamp, LastModifiedTimestamp, QueueArn */
	Attrs []Attribute `xml:"Attribute,omitempty"`
}

type ListQueueTagsResponse struct {
	Xmlns    string            `xml:"xmlns,attr,omitempty"`
	Result  ListQueueTagsResult `xml:"ListQueueTagsResult"`
	Metadata ResponseMetadata  `xml:"ResponseMetadata,omitempty"`
}

type ListQueueTagsResult struct {
	Tag []Tag `xml:"Tag"`
}

type Tag struct {
	Key string `xml:"Key"`
	Value string `xml:"Value"`
}

type GetQueueAttributesResponse struct {
	Xmlns    string                   `xml:"xmlns,attr,omitempty"`
	Result   GetQueueAttributesResult `xml:"GetQueueAttributesResult"`
	Metadata ResponseMetadata         `xml:"ResponseMetadata,omitempty"`
}

type SetQueueAttributesResponse struct {
	Xmlns    string           `xml:"xmlns,attr,omitempty"`
	Metadata ResponseMetadata `xml:"ResponseMetadata,omitempty"`
}
