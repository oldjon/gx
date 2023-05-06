package service

import (
	"fmt"
	"strconv"

	gTags "github.com/oldjon/gx/modules/grpc/tags"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type eventLoggerChannel uint8

const (
	eventLoggerChannelFile eventLoggerChannel = iota
)

// EventLogger log event object
type EventLogger struct {
	metadata map[string]string

	loggers map[eventLoggerChannel]*zap.Logger
	// tags may be nil
	tags gTags.Tags
}

// F stands for Field, it logs event object by listing event fields one by one
func (el EventLogger) F(event string, fields ...zapcore.Field) {
	payload := zapcore.ObjectMarshalerFunc(func(enc zapcore.ObjectEncoder) error {
		for _, field := range fields {
			field.AddTo(enc)
		}
		return nil
	})
	el.O(event, payload)
}

// O stands for Object, it logs event object that implements zapcore.ObjectMarshaler interface
func (el EventLogger) O(event string, o zapcore.ObjectMarshaler) {
	el.log(event, o, el.tags)
}

// CO stands for Object. same as O but add new param gTags.
func (el EventLogger) CO(event string, o zapcore.ObjectMarshaler, gtags gTags.Tags) {
	el.log(event, o, gtags)
}

func (el EventLogger) log(event string, o zapcore.ObjectMarshaler, t gTags.Tags) {
	eventID := strconv.FormatUint(randomID(), 36)
	for channel, logger := range el.loggers {
		switch channel {
		case eventLoggerChannelFile:
			el.logIntoFile(logger, eventID, event, o, t)
		}
	}
}

func (el EventLogger) logIntoFile(logger *zap.Logger, eventID string, event string, o zapcore.ObjectMarshaler, t gTags.Tags) {

	fields := []zapcore.Field{zap.String("id", eventID)}

	if len(el.metadata) != 0 {
		fields = append(fields, zap.Object("metadata", metadata(el.metadata)))
	}

	if t != nil && t.Len() != 0 {
		fields = append(fields, zap.Object("context", tags{t}))
	}
	fields = append(fields, zap.Object("payload", o))
	logger.Info(event, fields...)
}

func extractAccountIDFromContextTags(t gTags.Tags) *string {
	if t != nil {
		if t.Has("player_id") {
			playerIDStr, ok := t.Get("player_id").(string)
			if ok {
				return &playerIDStr
			}

			playerIDF, ok := t.Get("player_id").(float64)
			if ok {
				idStr := strconv.FormatUint(uint64(playerIDF), 10)
				return &idStr
			}

			playerID, ok := t.Get("player_id").(uint64)
			if ok {
				idStr := strconv.FormatUint(playerID, 10)
				return &idStr
			}
		} else if t.Has("account_id") {
			accountIDStr, ok := t.Get("account_id").(string)
			if ok {
				return &accountIDStr
			}

			accountIDF, ok := t.Get("account_id").(float64)
			if ok {
				idStr := strconv.FormatUint(uint64(accountIDF), 10)
				return &idStr
			}

			accountID, ok := t.Get("account_id").(uint64)
			if ok {
				idStr := strconv.FormatUint(accountID, 10)
				return &idStr
			}
		}
	}

	return nil
}

func (el EventLogger) Sync() error {
	for _, logger := range el.loggers {
		err := logger.Sync()
		if err != nil {
			fmt.Printf("failed to call sync event logger: %v+", err)
		}
	}

	return nil
}

func (el *EventLogger) SetTags(tags gTags.Tags) *EventLogger {
	el.tags = tags
	return el
}

func (el *EventLogger) Clone() *EventLogger {
	copy := *el
	return &copy
}

type metadata map[string]string

func (md metadata) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for k, v := range md {
		enc.AddString(k, v)
	}
	return nil
}

type tags struct {
	tags gTags.Tags
}

func (t tags) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	_ = t.tags.Foreach(func(key string, val interface{}) error {
		_ = enc.AddReflected(key, val)
		return nil
	})
	return nil
}
