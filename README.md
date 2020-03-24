logrus_firehose
====

 [![GoDoc](https://godoc.org/github.com/beaubrewer/logrus_firehose?status.svg)](https://godoc.org/github.com/beaubrewer/logrus_firehose)


# AWS Firehose Hook for Logrus <img src="http://i.imgur.com/hTeVwmJ.png" width="40" height="40" alt=":walrus:" class="emoji" title=":walrus:"/>

## Usage

```go
import (
    "github.com/sirupsen/logrus"
    "github.com/beaubrewer/logrus_firehose"
)

func main() {
    hook, err := logrus_firehose.New("my_stream", Config{
        AccessKey: "ABC", // AWS accessKeyId
        SecretKey: "XYZ", // AWS secretAccessKey
        Region:    "us-west-2",
    })

    // set custom fire level
    hook.SetLevels([]logrus.Level{
        logrus.PanicLevel,
        logrus.ErrorLevel,
    })
    
    hook.
    

    // send log with logrus
    logger := logrus.New()
    logger.Hooks.Add(hook)
    logger.WithFields(f).Error("my_message") // send log data to firehose as JSON
}
```


## Special fields

Some logrus fields have a special meaning in this hook.

|||
|:--|:--|
|`message`|if `message` is not set, entry.Message is added to log data in "message" field. |
|`stream_name`|`stream_name` isthe  stream name for Firehose. If not set, `defaultStreamName` is used as stream name.|
logrus_firehose
====

 [![GoDoc](https://godoc.org/github.com/beaubrewer/logrus_firehose?status.svg)](https://godoc.org/github.com/beaubrewer/logrus_firehose)


# AWS Firehose Hook for Logrus <img src="http://i.imgur.com/hTeVwmJ.png" width="40" height="40" alt=":walrus:" class="emoji" title=":walrus:"/>

## Usage

```go
import (
    "github.com/sirupsen/logrus"
    "github.com/beaubrewer/logrus_firehose"
)

func main() {
    hook, err := logrus_firehose.New("my_stream", logrus_firehose.Config{
        AccessKey: "ABC", // AWS accessKeyId
        SecretKey: "XYZ", // AWS secretAccessKey
        Region:    "us-west-2",
        Endpoint:  "firehose.us-west-2.amazonaws.com",
    })

    // set custom fire level
    hook.SetLevels([]logrus.Level{
        logrus.PanicLevel,
        logrus.ErrorLevel,
    })

    // ignore field
    hook.AddIgnore("context")

    // add custome filter
    hook.AddFilter("error", logrus_firehose.FilterError)


    // send log with logrus
    logger := logrus.New()
    logger.Hooks.Add(hook)
    logger.WithFields(f).Error("my_message") // send log data to firehose as JSON
}
```


## Special fields

Some logrus fields have a special meaning in this hook.

|||
|:--|:--|
|`message`|if `message` is not set, entry.Message is added to log data in "message" field. |
|`stream_name`|`stream_name` isthe  stream name for Firehose. If not set, `defaultStreamName` is used as stream name.|
