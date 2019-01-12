// Package config ...
package config

import (
	"time"
)

type Watcher interface {
	Watch() <-chan []byte
}

// Configer ...
type Configer interface {
	// load config content
	Load() error
	// Return when config changed
	LastModify() (time.Time, error)


	GetAllSections() map[string]Sectioner
	GetSection(sec string) (Sectioner, error)
	GetIntSetting(sec, key string) (val int64, err error)
	GetBoolSetting(sec, key string) (val bool, err error)
	GetFloatSetting(sec, key string) (val float64, err error)
	GetSetting(sec, key string) (val string, err error)
}

// Sectioner contains key-value of each module
type Sectioner interface {
	GetInt(key string) (val int64, err error)
	GetBool(key string) (val bool, err error)
	GetFloat(key string) (val float64, err error)
	GetString(key string) (val string, err error)

	// Return defaultVal when key missed
	GetIntMust(key string, defaultVal int64) int64
	GetBoolMust(key string, defaultVal bool) bool
	GetFloatMust(key string, defaultVal float64) float64
	GetStringMust(key string, defaultVal string) string
	MapTo() map[string]string
}
