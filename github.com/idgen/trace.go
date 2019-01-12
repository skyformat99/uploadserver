package idgen

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	errNotFoundIntf = errors.New("not found avaliabe interface")
)

var (
	defaultTraceGen *traceGen
	once            sync.Once
	defaultIP       string
)

func init() {
	ip, err := getLocalIP()
	if err != nil {
		ip = "127.0.0.1"
	}
	defaultIP = ip
}

// NewTraceGen ...
func NewTraceGen() error {
	pid := os.Getpid()
	locIP, err := getLocalIP()
	if err != nil {
		return err
	}
	ipFs := strings.Split(locIP, ".")
	if len(ipFs) != 4 {
		return errors.New("invalid ip:" + locIP)
	}
	ipData := make([]byte, 4)
	for i, fs := range ipFs {
		val, _ := strconv.ParseInt(fs, 10, 64)
		ipData[i] = byte(val)
	}
	once.Do(func() {
		defaultTraceGen = &traceGen{
			pid:    pid,
			ipData: ipData,
			mu:     new(sync.Mutex),
		}
		defaultTraceGen.ipInt64, _ = binary.Uvarint(ipData)
	})
	return nil
}

type traceGen struct {
	pid     int
	ipData  []byte
	ipInt64 uint64

	mu  *sync.Mutex
	ts  int64
	seq uint32
}

// GenTraceID ...
func GenTraceID() string {
	if defaultTraceGen == nil {
		err := NewTraceGen()
		if err != nil {
			return ""
		}
	}
	tg := defaultTraceGen
	tg.mu.Lock()
	defer tg.mu.Unlock()

	ts := time.Now().Unix()
	if tg.ts == ts {
		tg.seq++
	} else {
		tg.ts = ts
		tg.seq = 0
	}
	buf := &bytes.Buffer{}
	buf.Write(tg.ipData)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(ts))
	buf.Write(b)
	buf.Write([]byte{byte(0), byte(0)})
	b = make([]byte, 2)
	binary.BigEndian.PutUint16(b, uint16(tg.pid))
	buf.Write(b)
	b = make([]byte, 4)
	seqAndTag := tg.seq<<8 | 0xb0
	binary.BigEndian.PutUint32(b, uint32(seqAndTag))
	buf.Write(b)
	return hex.EncodeToString(buf.Bytes())
}

func getLocalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue
		}
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}
	return "", errNotFoundIntf
}

func stringIP2IntIP(ipString string) int {
	ipSegs := strings.Split(ipString, ".")
	var ipInt int
	var offset uint = 24
	for _, ipSeg := range ipSegs {
		tempInt, _ := strconv.Atoi(ipSeg)
		tempInt = tempInt << offset
		ipInt = ipInt | tempInt
		offset -= 8
	}
	return ipInt
}

func parseUnixNanoTimeToSecond(unixTimestamp int64) int64 {
	return unixTimestamp / 1000 / 1000 / 1000
}

func parseUnixNanoTimeToMilSecond(unixTimestamp int64) int64 {
	return unixTimestamp % (1000 * 1000 * 1000) / 1000
}

// GenSpanID ...
func GenSpanID() string {
	unixNanoTimestamp := time.Now().UnixNano()
	timeNum := parseUnixNanoTimeToSecond(unixNanoTimestamp) + parseUnixNanoTimeToMilSecond(unixNanoTimestamp)
	randNum := rand.Int31()
	ipString := defaultIP
	if ipString == "" {
		ipString = "127.0.0.1"
	}
	ipNum := stringIP2IntIP(ipString)
	return fmt.Sprintf("%08s", strconv.FormatInt(int64(ipNum)^timeNum, 16)) + fmt.Sprintf("%08s", strconv.FormatInt(int64(randNum), 16))
}
