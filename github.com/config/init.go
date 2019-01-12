// Package config ...
package config

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)

type iniSection map[string]string

type iniConf struct {
	suffix   string
	confPath string
	secs     map[string]iniSection
}

func newIni(path string) (ini *iniConf, err error) {
	ini = &iniConf{
		suffix:   "conf",
		confPath: path,
	}
	err = ini.Load()
	return ini, err
}



func (ic *iniConf) Load() error {
	fd, err := os.Open(ic.confPath)
	if err != nil {
		return err
	}
	defer fd.Close()
	secs := make(map[string]iniSection)
	reader := bufio.NewReader(fd)
	secName := ""
	for {
		line, prefix, err := reader.ReadLine()
		if err != nil {
			break
		}
		// TODO
		_ = prefix
		lineStr := string(line)
		lineStr = strings.Trim(lineStr, "\r\n")
		lineStr = strings.TrimSpace(lineStr)
		if lineStr == "" {
			continue
		}
		if strings.HasPrefix(lineStr, "\"") || strings.HasPrefix(lineStr, "#") {
			continue
		}
		if strings.HasPrefix(lineStr, "[") && strings.HasSuffix(lineStr, "]") {
			secName = string(line[1 : len(line)-1])
			secs[secName] = make(iniSection)
			continue
		}
		if secName == "" {
			return fmt.Errorf("bad conf format, miss section")
		}
		kv := strings.SplitN(lineStr, "=", 2)
		if len(kv) != 2 {
			return fmt.Errorf("bad conf format, kv should separate by =, but got %v", kv)
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		secs[secName][key] = value
	}
	if err == io.EOF {
		err = nil
	}
	ic.secs = secs
	return err
}

func (ic *iniConf) LastModify() (time.Time, error) {
	info, err := os.Stat(ic.confPath)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime(), nil
}

func (ic *iniConf) GetAllSections() map[string]Sectioner {
	secs := make(map[string]Sectioner)
	for s := range ic.secs {
		secs[s] = ic.secs[s]
	}
	return secs
}

func (ic *iniConf) GetSection(secName string) (Sectioner, error) {
	if sec, ok := ic.secs[secName]; ok {
		return sec, nil
	}
	return nil, wrapMissKeyErr(secName)
}


func (ic *iniConf) GetSetting(secName, key string) (string, error) {
	sec, err := ic.GetSection(secName)
	if err != nil {
		return "", err
	}
	return sec.GetString(key)
}

func (ic *iniConf) GetIntSetting(secName, key string) (int64, error) {
	sec, err := ic.GetSection(secName)
	if err != nil {
		return 0, err
	}
	return sec.GetInt(key)
}

func (ic *iniConf) GetFloatSetting(secName, key string) (float64, error) {
	sec, err := ic.GetSection(secName)
	if err != nil {
		return 0, err
	}
	return sec.GetFloat(key)
}

func (ic *iniConf) GetBoolSetting(secName, key string) (bool, error) {
	sec, err := ic.GetSection(secName)
	if err != nil {
		return false, err
	}
	return sec.GetBool(key)
}

func (is iniSection) GetString(key string) (string, error) {
	if val, ok := is[key]; ok {
		return val, nil
	}
	return "", wrapMissKeyErr(key)
}

func (is iniSection) GetInt(key string) (int64, error) {
	if val, ok := is[key]; ok {
		return strconv.ParseInt(val, 10, 64)
	}
	return 0, wrapMissKeyErr(key)
}

func (is iniSection) GetFloat(key string) (float64, error) {
	if val, ok := is[key]; ok {
		return strconv.ParseFloat(val, 64)
	}
	return 0.0, wrapMissKeyErr(key)
}

func (is iniSection) GetBool(key string) (bool, error) {
	if _, ok := is[key]; !ok {
		return false, wrapMissKeyErr(key)
	}
	switch strings.ToUpper(is[key]) {
	case "TRUE":
		return true, nil
	case "FALSE":
		return false, nil
	}
	return false, fmt.Errorf("type error: not bool: %s", is[key])
}

func (is iniSection) GetStringMust(key string, defaultValue string) string {
	val, err := is.GetString(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (is iniSection) GetIntMust(key string, defaultValue int64) int64 {
	val, err := is.GetInt(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (is iniSection) GetBoolMust(key string, defaultValue bool) bool {
	val, err := is.GetBool(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (is iniSection) GetFloatMust(key string, defaultValue float64) float64 {
	val, err := is.GetFloat(key)
	if err != nil {
		val = defaultValue
	}
	return val
}

func (is iniSection) MapTo() map[string]string {
	res := make(map[string]string, 0)
	for k, v := range is {
		res[k] = v
	}
	return res
}
