// Package config ...
package config

import (
	"fmt"
)

type confFormatType uint8

const (
	// ConfFormatTypeIni ini
	ConfFormatTypeIni confFormatType = iota
	// ConfFormatTypeToml toml
	ConfFormatTypeToml
	// ConfFormatTypeEtcd etcd
	ConfFormatTypeEtcd
)

// New return default ini-configer
func New(confPath string) (Configer, error) {
	return newIni(confPath)
}

// NewConfigWithFormatType new configer according to conf-type
func NewConfigWithFormatType(ft confFormatType, confPath string) (Configer, error) {
	switch ft {
	case ConfFormatTypeIni:
		return newIni(confPath)
	case ConfFormatTypeToml:
		return newToml(confPath)
	case ConfFormatTypeEtcd:
		return nil, fmt.Errorf("not implemented")
	default:
		return nil, fmt.Errorf("unsupported confFormatType:%d", ft)
	}
}
