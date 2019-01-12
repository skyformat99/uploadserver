// Package config ...
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

type tomlConf struct {
	suffix   string
	confPath string
	secs     map[string]tomlSection
	meta     toml.MetaData
}

type tomlSection map[string]interface{}

func newToml(conf string) (*tomlConf, error) {
	tc := &tomlConf{suffix: "toml", confPath: conf}
	err := tc.Load()
	return tc, err
}


func (tc *tomlConf) Load() error {
	ret := make(map[string]tomlSection)
	mt, err := toml.DecodeFile(tc.confPath, &ret)
	if err != nil {
		return err
	}
	tc.secs = ret
	tc.meta = mt
	return nil
}

func (tc *tomlConf) LastModify() (time.Time, error) {
	info, err := os.Stat(tc.confPath)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

func (tc *tomlConf) GetSection(secName string) (Sectioner, error) {
	if sec, ok := tc.secs[secName]; ok {
		return sec, nil
	}
	return nil, wrapMissKeyErr(secName)
}

func (tc *tomlConf) GetAllSections() map[string]Sectioner {
	secs := make(map[string]Sectioner)
	for k, sec := range tc.secs {
		secs[k] = sec
	}
	return secs
}



func (tc *tomlConf) GetSetting(secName, key string) (string, error) {
	sec, err := tc.GetSection(secName)
	if err != nil {
		return "", err
	}
	return sec.GetString(key)
}

func (tc *tomlConf) GetIntSetting(secName, key string) (int64, error) {
	sec, err := tc.GetSection(secName)
	if err != nil {
		return 0, err
	}
	return sec.GetInt(key)
}

func (tc *tomlConf) GetFloatSetting(secName, key string) (float64, error) {
	sec, err := tc.GetSection(secName)
	if err != nil {
		return 0, err
	}
	return sec.GetFloat(key)
}

func (tc *tomlConf) GetBoolSetting(secName, key string) (bool, error) {
	sec, err := tc.GetSection(secName)
	if err != nil {
		return false, err
	}
	return sec.GetBool(key)
}

func (ts tomlSection) GetString(key string) (string, error) {
	if val, ok := ts[key]; ok {
		switch x := val.(type) {
		case string:
			return x, nil
		default:
			return fmt.Sprintf("%v", x), nil
		}
	}
	return "", wrapMissKeyErr(key)
}

func (ts tomlSection) GetInt(key string) (int64, error) {
	if val, ok := ts[key]; ok {
		switch x := val.(type) {
		case string:
			return strconv.ParseInt(x, 10, 64)
		case int:
			return int64(x), nil
		case int64:
			return x, nil
		default:
			return 0, fmt.Errorf("not int64, but %T", x)
		}
	}
	return 0, wrapMissKeyErr(key)
}

func (ts tomlSection) GetFloat(key string) (float64, error) {
	if val, ok := ts[key]; ok {
		switch x := val.(type) {
		case string:
			return strconv.ParseFloat(x, 64)
		case float64:
			return x, nil
		case int64:
			return float64(x), nil
		default:
			return 0.0, fmt.Errorf("not float64, but %T", x)
		}
	}
	return 0.0, wrapMissKeyErr(key)
}

func (ts tomlSection) GetBool(key string) (bool, error) {
	if _, ok := ts[key]; !ok {
		return false, wrapMissKeyErr(key)
	}
	switch x := ts[key].(type) {
	case bool:
		return x, nil
	case string:
		return strToBool(x)
	default:
		return false, fmt.Errorf("not bool, but %T", x)
	}
}

func (ts tomlSection) GetStringMust(key string, defaultValue string) string {
	val, err := ts.GetString(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (ts tomlSection) GetIntMust(key string, defaultValue int64) int64 {
	val, err := ts.GetInt(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (ts tomlSection) GetBoolMust(key string, defaultValue bool) bool {
	val, err := ts.GetBool(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (ts tomlSection) GetFloatMust(key string, defaultValue float64) float64 {
	val, err := ts.GetFloat(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (ts tomlSection) MapTo() map[string]string {
	res := make(map[string]string, 0)
	for k, v := range ts {
		res[k] = v.(string)
	}
	return res
}


func strToBool(str string) (bool, error) {
	switch strings.ToUpper(str) {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	}
	return false, fmt.Errorf("type error: not bool: %s", str)
}
