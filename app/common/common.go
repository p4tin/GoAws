package common

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"hash"
	"io"
	"sort"
	"strings"
	log "github.com/sirupsen/logrus"

	"github.com/p4tin/goaws/app"
)

var LogMessages bool
var LogFile string

func NewUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(rand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	// variant bits; see section 4.1.1
	uuid[8] = uuid[8]&^0xc0 | 0x80
	// version 4 (pseudo-random); see section 4.1.3
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

func GetMD5Hash(text string) string {
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func HashAttributes(attributes map[string]app.MessageAttributeValue) string {
	hasher := md5.New()

	keys := sortedKeys(attributes)
	for _, key := range keys {
		attributeValue := attributes[key]

		addStringToHash(hasher, key)
		addStringToHash(hasher, attributeValue.DataType)
		if attributeValue.ValueKey == "StringValue" {
			hasher.Write([]byte{1})
			addStringToHash(hasher, attributeValue.Value)
		} else if attributeValue.ValueKey == "BinaryValue" {
			hasher.Write([]byte{2})
			bytes, _ := base64.StdEncoding.DecodeString(attributeValue.Value)
			addBytesToHash(hasher, bytes)
		}
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func DeriveQueueUrl(queueUrl string, req *http.Request) string {
	//check if we get a forwarded proto, honor it if we do
	externalProto := req.Header.Get("X-Forwarded-Proto")
	if len(externalProto) == 0 {
		externalProto = "http"
	}
	derivedQueueUrl := strings.Replace(queueUrl, "http://" + app.CurrentEnvironment.Host + ":" + app.CurrentEnvironment.Port, externalProto + "://" + req.Host, -1)
	log.Debugf("Derived new queue URL: %s from request: %s with original: %s", derivedQueueUrl, req.Host, queueUrl)
	return derivedQueueUrl
}

func sortedKeys(attributes map[string]app.MessageAttributeValue) []string {
	var keys []string
	for key := range attributes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func addStringToHash(hasher hash.Hash, str string) {
	bytes := []byte(str)
	addBytesToHash(hasher, bytes)
}

func addBytesToHash(hasher hash.Hash, arr []byte) {
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(len(arr)))
	hasher.Write(bs)
	hasher.Write(arr)
}
