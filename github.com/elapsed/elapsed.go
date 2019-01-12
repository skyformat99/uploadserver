package elapsed

import (
	"time"
)

// be happy for test
var timeNow = time.Now

//ElapTime : elapsed time struct
type ElapTime struct {
	start   time.Time
	elapsed time.Duration
}

//New : create ElapTime struct
func New() *ElapTime {
	return &ElapTime{}
}

//Start : fill start time
func (et *ElapTime) Start() {
	et.start = timeNow()
}

//Stop : fill elapsed time
func (et *ElapTime) Stop() time.Duration {
	et.elapsed = timeNow().Sub(et.start)
	return et.elapsed
}

//Elapsed : return elapsed time
func (et *ElapTime) Elapsed() time.Duration {
	return et.elapsed
}

//Reset : clear start and elapsed time
func (et *ElapTime) Reset() {
	et.start = time.Time{}
	et.elapsed = 0
}

//String : format elapsed time as string
func (et *ElapTime) String() string {
	return et.elapsed.String()
}

// StopAndRestart ....
func (et *ElapTime) StopAndRestart() time.Duration {
	cost := et.Stop()
	et.Reset()
	et.Start()
	return cost
}
