package logrus_firehose

import (
	"encoding/json"
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/firehose"
)

var defaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

// FirehoseHook is logrus hook for AWS Firehose.
// Amazon Kinesis Firehose is a fully-managed service that delivers real-time
// streaming data to destinations such as Amazon Simple Storage Service (Amazon
// S3), Amazon Elasticsearch Service (Amazon ES), and Amazon Redshift.
type FirehoseHook struct {
	client              *firehose.Firehose
	defaultStreamName   string
	defaultPartitionKey string
	async               bool
	levels              []logrus.Level
	ignoreFields        map[string]struct{}
	filters             map[string]func(interface{}) interface{}
	addNewline          bool
}

// New returns initialized logrus hook for Firehose with persistent Firehose logger.
func New(name string, conf Config) (*FirehoseHook, error) {
	sess, err := session.NewSession(conf.AWSConfig())
	if err != nil {
		return nil, err
	}

	svc := firehose.New(sess)
	return &FirehoseHook{
		client:            svc,
		defaultStreamName: name,
		levels:            defaultLevels,
		ignoreFields:      make(map[string]struct{}),
		filters:           make(map[string]func(interface{}) interface{}),
	}, nil
}

// NewWithConfig returns initialized logrus hook for Firehose with persistent Firehose logger.
func NewWithAWSConfig(name string, conf *aws.Config) (*FirehoseHook, error) {
	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	svc := firehose.New(sess)
	return &FirehoseHook{
		client:       svc,
		levels:       defaultLevels,
		ignoreFields: make(map[string]struct{}),
		filters:      make(map[string]func(interface{}) interface{}),
	}, nil
}

// Levels returns logging level to fire this hook.
func (h *FirehoseHook) Levels() []logrus.Level {
	return h.levels
}

// SetLevels sets logging level to fire this hook.
func (h *FirehoseHook) SetLevels(levels []logrus.Level) {
	h.levels = levels
}

// Async sets async flag and send log asynchroniously.
// If use this option, Fire() does not return error.
func (h *FirehoseHook) Async() {
	h.async = true
}

// AddIgnore adds field name to ignore.
func (h *FirehoseHook) AddIgnore(name string) {
	h.ignoreFields[name] = struct{}{}
}

// AddFilter adds a custom filter function.
func (h *FirehoseHook) AddFilter(name string, fn func(interface{}) interface{}) {
	h.filters[name] = fn
}

// AddNewline sets if a newline is added to each message.
func (h *FirehoseHook) AddNewLine(b bool) {
	h.addNewline = b
}

// Fire is invoked by logrus and sends log to Firehose.
func (h *FirehoseHook) Fire(entry *logrus.Entry) error {
	if !h.async {
		return h.fire(entry)
	}

	// send log asynchroniously and return no error.
	go h.fire(entry)
	return nil
}

// Fire is invoked by logrus and sends log to Firehose.
func (h *FirehoseHook) fire(entry *logrus.Entry) error {
	in := &firehose.PutRecordInput{
		DeliveryStreamName: stringPtr(h.getStreamName(entry)),
		Record: &firehose.Record{
			Data: h.getData(entry),
		},
	}
	_, err := h.client.PutRecord(in)
	return err
}

func (h *FirehoseHook) getStreamName(entry *logrus.Entry) string {
	if name, ok := entry.Data["stream_name"].(string); ok {
		return name
	}
	return h.defaultStreamName
}

func (h *FirehoseHook) getData(entry *logrus.Entry) []byte {
	data := make(logrus.Fields)
	for k, v := range entry.Data {
		if _, ok := h.ignoreFields[k]; ok {
			continue
		}
		if fn, ok := h.filters[k]; ok {
			v = fn(v) // apply custom filter
		} else {
			v = formatData(v) // use default formatter
		}
		data[k] = v
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	if h.addNewline {
		n := []byte("\n")
		bytes = append(bytes, n...)
	}
	return bytes
}

// formatData returns value as a suitable format.
func formatData(value interface{}) (formatted interface{}) {
	switch value := value.(type) {
	case json.Marshaler:
		return value
	case error:
		return value.Error()
	case fmt.Stringer:
		return value.String()
	default:
		return value
	}
}

func stringPtr(str string) *string {
	return &str
}
