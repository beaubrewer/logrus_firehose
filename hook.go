package logrus_firehose

import (
	"time"

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

/*
	firehose batch put request can send up to 500 events
*/
const firehoseMaxBatchSize = 500

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
		factory method for logrus.Formatter
	*/
	formatterFactory func() logrus.Formatter

	/*
		blockingMode specify if the queue is not empty
		should any write being blocked or discard
	*/
	blockingMode bool

	/*
		specify nb of msgs to be send as firehose batch
	*/
	sendBatchSize int

	/*
		async queue used to
	*/
	sendQueue chan *logrus.Entry

	/*
		a logger for error operation
		DO NOT use the same logger being hook onto
	*/
	logger logrus.FieldLogger

	/*
		nb of worker to send firehose event
	*/
	numWorker int
}

// NewFirehoseHook returns initialized logrus hook for Firehose with persistent Firehose logger.
func NewFirehoseHook(name string, client *firehose.Firehose, opts ...Option) (*FirehoseHook, error) {
	hk := &FirehoseHook{
		client:           client,
		streamName:       name,
		levels:           DefaultLevels,
		formatterFactory: func() logrus.Formatter { return &logrus.JSONFormatter{} },
		sendBatchSize:    firehoseMaxBatchSize,
		sendQueue:        make(chan *logrus.Entry, firehoseMaxBatchSize),
		numWorker:        1,
		blockingMode:     false,
		addNewline:       false,
	}
	for _, opt := range opts {
		opt(hk)
	}
	return hk, nil
}

// WithLevels sets logging level to fire this hook.
func WithLevels(levels []logrus.Level) Option {
	return func(hook *FirehoseHook) {
		hook.levels = levels
	}
}

// WithAddNewline sets if a newline is added to each message.
func WithAddNewLine() Option {
	return func(hook *FirehoseHook) {
		hook.addNewline = true
	}
}

// WithFormatter sets a log entry formatter
func WithFormatterFactory(f func() logrus.Formatter) Option {
	return func(hook *FirehoseHook) {
		hook.formatterFactory = f
	}
}

// WithLogger set a logger
// DO NOT use the same logger where it's being hooked on
func WithLogger(l logrus.FieldLogger) Option {
	return func(hook *FirehoseHook) {
		hook.logger = l
	}
}

func WithBlockingMode(mode bool) Option {
	return func(hook *FirehoseHook) {
		hook.blockingMode = mode
	}
}

func WithSendBatchSize(size int) Option {
	if size > firehoseMaxBatchSize || size < 1 {
		panic("invalid batch size specified")
	}
	return func(hook *FirehoseHook) {
		hook.sendBatchSize = size
		hook.sendQueue = make(chan *logrus.Entry, size)
	}
}

var newLine = []byte("\n")

/*
	formatEntry formats the log entry.
	this method is not concurrent safe
*/
func (h *FirehoseHook) formatEntry(f logrus.Formatter, entry *logrus.Entry) []byte {
	bytes, err := f.Format(entry)
	if err != nil {
		return nil
	}
	if h.addNewline {
		bytes = append(bytes, newLine...)
	}
	return bytes
}

// Levels returns logging level to fire this hook.
func (h *FirehoseHook) Levels() []logrus.Level {
	return h.levels
}

// Fire is invoked by logrus and sends log to Firehose.
func (h *FirehoseHook) Fire(entry *logrus.Entry) error {
	for {
		select {
		case h.sendQueue <- entry:
			return nil
		default:
			if !h.blockingMode {
				if h.logger != nil {
					h.logger.Warn("queue is full and non-blocking mode specified, dropping record")
				}
				return nil
			}
		}
	}
}

func (h *FirehoseHook) SendLoop(tick <-chan time.Time) {
	for i := 0; i < h.numWorker; i++ {
		go func() {
			// do not share formatter cross workers
			formatter := h.formatterFactory()
			for {
				buf := make([]*firehose.Record, 0, h.sendBatchSize)

				select {
				case <-tick:
					break
				case entry := <-h.sendQueue:
					buf = append(buf, &firehose.Record{Data: h.formatEntry(formatter, entry)})
					if len(buf) >= h.sendBatchSize {
						break
					}
				default:
					if len(buf) >= h.sendBatchSize {
						break
					}
				}
				if len(buf) == 0 {
					continue
				}
				resp, err := h.client.PutRecordBatch(
					&firehose.PutRecordBatchInput{
						DeliveryStreamName: aws.String(h.streamName),
						Records:            buf,
					},
				)
				if err == nil && *resp.FailedPutCount == 0 {
					if h.logger != nil {
						h.logger.WithField("lines-emitted", len(resp.RequestResponses)).
							Debug("log successfully emitted")
					}
					continue
				}
				if h.logger != nil {
					h.logger.WithError(err).
						WithField("failed-rec-count", *resp.FailedPutCount).
						Warn("failed to send logs to firehose")
				}
			}
		}()
	}
}

var _ logrus.Hook = (*FirehoseHook)(nil)
