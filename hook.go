package logrus_firehose

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/firehose"
	"github.com/sirupsen/logrus"
)

var DefaultLevels = []logrus.Level{
	logrus.PanicLevel,
	logrus.FatalLevel,
	logrus.ErrorLevel,
	logrus.WarnLevel,
	logrus.InfoLevel,
}

type Option func(*FirehoseHook)

// FirehoseHook is logrus hook for AWS Firehose.
// Amazon Kinesis Firehose is a fully-managed service that delivers real-time
// streaming data to destinations such as Amazon Simple Storage Service (Amazon
// S3), Amazon Elasticsearch Service (Amazon ES), and Amazon Redshift.
type FirehoseHook struct {

	/*
		firehose client
	*/
	client *firehose.Firehose

	/*
		firehose stream name to write to
	*/
	streamName string

	/*
		levels being hooked
	*/
	levels []logrus.Level

	/*
		should append new line in each msg
	*/
	addNewline bool

	/*
		an instance of logrus.Formatter used to format the msg
	*/
	formatter logrus.Formatter

	/*
		make firehose request in async mode
	*/
	async bool

	/*
		blockingMode specify if the queue is not empty
		should any write being blocked or discard
	*/
	blockingMode bool

	/*
		async queue used to
	*/
	sendQueue chan interface{}
}

// NewWithConfig returns initialized logrus hook for Firehose with persistent Firehose logger.
func NewFirehoseHook(name string, client *firehose.Firehose, opts ...Option) (*FirehoseHook, error) {
	hk := &FirehoseHook{
		client:     client,
		streamName: name,
		levels:     DefaultLevels,
		formatter:  &logrus.JSONFormatter{},
	}
	for _, opt := range opts {
		opt(hk)
	}
	return hk, nil
}

// Levels returns logging level to fire this hook.
func (h *FirehoseHook) Levels() []logrus.Level {
	return h.levels
}

// WithLevels sets logging level to fire this hook.
func WithLevels(levels []logrus.Level) Option {
	return func(hook *FirehoseHook) {
		hook.levels = levels
	}
}

// WithAsync sets async flag and send log asynchroniously.
// If use this option, Fire() does not return error.
func WithAsync() Option {
	return func(hook *FirehoseHook) {
		hook.async = true
	}
}

// WithAddNewline sets if a newline is added to each message.
func (h *FirehoseHook) WithAddNewLine() Option {
	return func(hook *FirehoseHook) {
		h.addNewline = true
	}
}

// WithFormatter sets a log entry formatter
func (h *FirehoseHook) WithFormatter(f logrus.Formatter) Option {
	return func(hook *FirehoseHook) {
		hook.formatter = f
	}
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
		DeliveryStreamName: aws.String(h.streamName),
		Record: &firehose.Record{
			Data: h.getData(entry),
		},
	}
	_, err := h.client.PutRecord(in)
	return err
}

var newLine = []byte("\n")

func (h *FirehoseHook) getData(entry *logrus.Entry) []byte {
	bytes, err := h.formatter.Format(entry)
	if err != nil {
		return nil
	}
	if h.addNewline {
		bytes = append(bytes, newLine...)
	}
	return bytes
}
