package logrus_firehose

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)
	t.Skip("TODO: add some case for env and local credentials")

	hook, err := New("test_stream", Config{})
	assert.Error(err)
	assert.Nil(hook)
}

func TestNewWithAWSConfig(t *testing.T) {
	assert := assert.New(t)
	t.Skip("TODO: add some case")

	hook, err := NewWithAWSConfig("test_stream", nil)
	assert.Error(err)
	assert.Nil(hook)
}

func TestLevels(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		levels []logrus.Level
	}{
		{nil},
		{[]logrus.Level{logrus.WarnLevel}},
		{[]logrus.Level{logrus.ErrorLevel}},
		{[]logrus.Level{logrus.WarnLevel, logrus.DebugLevel}},
		{[]logrus.Level{logrus.WarnLevel, logrus.DebugLevel, logrus.ErrorLevel}},
	}

	for _, tt := range tests {
		target := fmt.Sprintf("%+v", tt)

		hook := FirehoseHook{}
		levels := hook.Levels()
		assert.Nil(levels, target)

		hook.levels = tt.levels
		levels = hook.Levels()
		assert.Equal(tt.levels, levels, target)
	}
}

func TestSetLevels(t *testing.T) {
	assert := assert.New(t)

	tests := []struct {
		levels []logrus.Level
	}{
		{nil},
		{[]logrus.Level{logrus.WarnLevel}},
		{[]logrus.Level{logrus.ErrorLevel}},
		{[]logrus.Level{logrus.WarnLevel, logrus.DebugLevel}},
		{[]logrus.Level{logrus.WarnLevel, logrus.DebugLevel, logrus.ErrorLevel}},
	}

	for _, tt := range tests {
		target := fmt.Sprintf("%+v", tt)

		hook := FirehoseHook{}
		assert.Nil(hook.levels, target)

		hook.SetLevels(tt.levels)
		assert.Equal(tt.levels, hook.levels, target)

		hook.SetLevels(nil)
		assert.Nil(hook.levels, target)
	}
}

func TestGetStreamName(t *testing.T) {
	assert := assert.New(t)

	emptyEntry := ""
	tests := []struct {
		hasEntryName bool
		entryName    interface{}
		defautName   string
		expectedName string
	}{
		{true, "entry_stream", "default_stream", "entry_stream"},
		{true, "entry_stream", "", "entry_stream"},
		{true, "", "default_stream", ""},
		{true, "", "", ""},
		{true, 99999, "default_stream", "default_stream"},
		{true, nil, "default_stream", "default_stream"},
		{false, emptyEntry, "default_stream", "default_stream"},
		{false, emptyEntry, "", ""},
	}

	for _, tt := range tests {
		target := fmt.Sprintf("%+v", tt)

		hook := FirehoseHook{
			defaultStreamName: tt.defautName,
		}
		entry := &logrus.Entry{
			Data: make(map[string]interface{}),
		}
		if tt.hasEntryName {
			entry.Data["stream_name"] = tt.entryName
		}

		assert.Equal(tt.expectedName, hook.getStreamName(entry), target)
	}
}
