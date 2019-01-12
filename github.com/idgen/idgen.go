package idgen

import (
	"errors"
	"net"
	"sync"
	"time"
)

// IDGenerater ...
type IDGenerater interface {
	NextID() (uint64, error)
}

// New ....
func New(ip string) IDGenerater {
	return newFlakeGen(ip)
}

const (
	// BitLenTime bit length of time
	BitLenTime = 39
	// BitLenSequence bit length of sequence number
	BitLenSequence = 8
	// BitLenMachineID bit length of machine id
	BitLenMachineID = 63 - BitLenTime - BitLenSequence
)

const (
	flakeTimeUnit = 1e7
	startSeed     = 128883497465
	maskSequence  = uint16(1<<BitLenSequence - 1)
)

type flakeGen struct {
	mu          *sync.Mutex
	startTime   int64
	elapsedTime int64
	sequence    uint16
	machineID   uint16
}

var _ IDGenerater = new(flakeGen)

func newFlakeGen(ip4 string) IDGenerater {
	ip := net.ParseIP(ip4).To4()
	mid := uint16(ip[2])<<8 + uint16(ip[3])
	fg := &flakeGen{
		mu:        new(sync.Mutex),
		sequence:  uint16(1<<BitLenSequence - 1),
		startTime: startSeed,
		machineID: mid,
	}
	return fg
}

func (fg *flakeGen) NextID() (uint64, error) {
	return fg.gen()
}

func (fg *flakeGen) gen() (uint64, error) {
	fg.mu.Lock()
	defer fg.mu.Unlock()

	t := time.Now()
	elapsed := t.UTC().UnixNano()/flakeTimeUnit - fg.startTime
	if fg.elapsedTime < elapsed {
		fg.elapsedTime = elapsed
		fg.sequence = 0
	} else {
		fg.sequence = (fg.sequence + 1) & maskSequence
		if fg.sequence == 0 {
			fg.elapsedTime++
			overtime := fg.elapsedTime - elapsed
			time.Sleep(time.Duration(overtime)*10*time.Millisecond -
				time.Duration(time.Now().UTC().UnixNano()%flakeTimeUnit)*time.Nanosecond)
		}
	}

	if fg.elapsedTime >= 1<<BitLenTime {
		return 0, errors.New("over time limit")
	}
	return uint64(fg.elapsedTime)<<(BitLenSequence+BitLenMachineID) |
		uint64(fg.sequence)<<BitLenMachineID |
		uint64(fg.machineID), nil
}
