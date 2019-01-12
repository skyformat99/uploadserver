// Package config ...
package config

import (
	"fmt"
)

func wrapMissKeyErr(key string) error {
	return fmt.Errorf("cannot find key: %s", key)
}
