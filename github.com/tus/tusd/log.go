package tusd

import (
	"github.com/logger"
)

func (h *UnroutedHandler) log(tag logger.TAIHETag, eventName string, details ...string) {
	LogEvent(tag, eventName, details...)
}

func LogEvent(tag logger.TAIHETag, eventName string, details ...string) {
	result := make([]byte, 0, 100)

	result = append(result, `event="`...)
	result = append(result, eventName...)
	result = append(result, `" `...)

	for i := 0; i < len(details); i += 2 {
		result = append(result, details[i]...)
		result = append(result, `="`...)
		result = append(result, details[i+1]...)
		result = append(result, `" `...)
	}

	result = append(result, "\n"...)
	logger.Info(tag, string(result))
}
