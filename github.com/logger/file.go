// Package logger ...
package logger

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/config"
)

var nowFunc = time.Now

type outer struct {
	level    logLevel
	fn       string
	setting  *fileSetting
	mu       *sync.Mutex
	fd       *os.File
	curSize  uint64
	curIndex int64
	curHour  string
}

type fileSetting struct {
	rmode        rotateMode
	dir          string
	rotateCount  int64
	rotateSize   uint64
	rotateByHour bool
	keepHours    int64
	seprated     bool
	autoClear    bool
	clearHours   int32
	clearStep    int32
	async        bool
	opt          *option
	disableLink  bool
	enable       bool
	format       string
	maxLevel     logLevel
	callDepth    int
}

type fileLog struct {
	input    chan *logCell
	out      *outer
	multiOut map[logLevel]*outer
	setting  *fileSetting
	close    chan struct{}
	wg       *sync.WaitGroup
}

type rotateMode uint8

const (
	_ rotateMode = iota
	rotateModeHour
	rotateModeSize
)

const (
	minRotateSize        = 1024 * 1024
	defaultInputChanSize = 1024 * 32
)

func parseFileSetting(sec config.Sectioner) *fileSetting {
	setting := &fileSetting{}
	cwd, _ := os.Getwd()
	setting.format = sec.GetStringMust("file.format", defaultFormat)
	setting.dir = sec.GetStringMust("file.dir", cwd)
	setting.async = sec.GetBoolMust("file.async", false)
	setting.autoClear = sec.GetBoolMust("file.auto_clear", false)
	setting.clearHours = int32(sec.GetIntMust("file.clear_hours", 24*30))
	setting.clearStep = int32(sec.GetIntMust("file.clear_step", 1))
	setting.disableLink = sec.GetBoolMust("file.disable_link", false)
	rbh := sec.GetBoolMust("file.rotate_by_hour", false)
	if rbh == false {
		rs, err := sec.GetInt("file.rotate_size")
		if err != nil {
			fmt.Fprintf(os.Stderr, "newFileLog err:%v", err)
			return nil
		}
		if rs < minRotateSize {
			rs = minRotateSize
		}
		setting.rmode = rotateModeSize
		setting.rotateSize = uint64(rs)
	} else {
		setting.rmode = rotateModeHour
	}
	level := strings.ToUpper(sec.GetStringMust("file.level", "DEBUG"))
	setting.maxLevel = getLogLevel(level)
	setting.seprated = sec.GetBoolMust("file.seprated", false)
	setting.enable = sec.GetBoolMust("file.enable", false)
	if !setting.enable {
		return nil
	}
	binName := filepath.Base(os.Args[0])
	setting.opt = &option{
		prefix: sec.GetStringMust("prefix", binName),
	}
	return setting
}

func newFileLog(sec config.Sectioner) Logger {
	fs := parseFileSetting(sec)
	if fs.callDepth == 0 {
		fs.callDepth = depth
	}
	return newFileLogWithSetting(fs)
}

func newFileLogWithSetting(fs *fileSetting) Logger {
	fl := &fileLog{
		close:   make(chan struct{}),
		wg:      new(sync.WaitGroup),
		input:   make(chan *logCell, defaultInputChanSize),
		setting: fs,
	}
	_, err := os.Lstat(fl.setting.dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(fl.setting.dir, 0777)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "newFileLog err:%v", err)
		return nil
	}
	fl.genOuter()
	go fl.writeLoop()
	return fl
}

func getFilesByDir(dir string) ([]string, error) {
	var fileList []string
	var walkFunc = func(path string, f os.FileInfo, err error) error {
		var strRet string
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}

		strRet += path
		fileList = append(fileList, strRet)
		return nil
	}
	err := filepath.Walk(dir, walkFunc)
	if err != nil {
		return nil, err
	}
	return fileList, nil
}

func (f *fileLog) Debug(args ...interface{}) {
	f.do(DEBUG, "", args...)
}

func (f *fileLog) Debugf(format string, args ...interface{}) {
	f.do(DEBUG, format, args...)
}

func (f *fileLog) Trace(args ...interface{}) {
	f.do(TRACE, "", args...)
}

func (f *fileLog) Tracef(format string, args ...interface{}) {
	f.do(TRACE, format, args...)
}

func (f *fileLog) Info(args ...interface{}) {
	f.do(INFO, "", args...)
}

func (f *fileLog) Infof(format string, args ...interface{}) {
	f.do(INFO, format, args...)
}

func (f *fileLog) Warn(args ...interface{}) {
	f.do(WARNING, "", args...)
}

func (f *fileLog) Warnf(format string, args ...interface{}) {
	f.do(WARNING, format, args...)
}

func (f *fileLog) Error(args ...interface{}) {
	f.do(ERROR, "", args...)
}

func (f *fileLog) Errorf(format string, args ...interface{}) {
	f.do(ERROR, format, args...)
}

func (f *fileLog) Fatal(args ...interface{}) {
	f.do(FATAL, "", args...)
}

func (f *fileLog) Fatalf(format string, args ...interface{}) {
	f.do(FATAL, format, args...)
}

func (f *fileLog) Close() {
	// TODO
	close(f.close)
	f.wg.Wait()
	if f.setting.seprated {
		for l := range f.multiOut {
			f.multiOut[l].fd.Close()
		}
	} else {
		f.out.fd.Close()
	}
}

func (f *fileLog) do(lev logLevel, format string, args ...interface{}) {
	if lev < f.setting.maxLevel || !f.setting.enable {
		return
	}
	msg := ""
	if format == "" {
		msg = fmt.Sprint(args...)
	} else {
		msg = fmt.Sprintf(format, args...)
	}
	lc := &logCell{
		level:    lev,
		callInfo: genCallInfo(f.setting.callDepth),
		format:   f.setting.format,
		msg:      msg,
	}
	if f.setting.async {
		f.input <- lc
		return
	}
	f.write(lc.level, formatLog(lc))
}

func (f *fileLog) genOuter() {
	if f.setting.seprated {
		f.multiOut = make(map[logLevel]*outer)
		var i logLevel
		wf := &outer{
			level:   WARNING,
			mu:      new(sync.Mutex),
			setting: f.setting,
		}
		wf.genFile()
		nl := &outer{
			level:   TRACE,
			mu:      new(sync.Mutex),
			setting: f.setting,
		}
		nl.genFile()
		for i = TRACE; i < WARNING; i++ {
			f.multiOut[i] = nl
		}
		for i = WARNING; i <= FATAL; i++ {
			f.multiOut[i] = wf
		}
	} else {
		f.out = &outer{
			mu:      new(sync.Mutex),
			setting: f.setting,
		}
		f.out.genFile()
	}
}

func (f *fileLog) writeLoop() {
	f.wg.Add(1)
	defer f.wg.Done()
	for {
		select {
		case r := <-f.input:
			f.write(r.level, formatLog(r))
		case <-f.close:
			return
		}
	}
}

func (f *fileLog) write(lev logLevel, data []byte) {
	out := f.out
	if f.setting.seprated {
		out = f.multiOut[lev]
	}
	switch f.setting.rmode {
	case rotateModeHour:
		cur := getCurHour(nowFunc())
		if cur != out.curHour {
			out.mu.Lock()
			out.curHour = cur
			out.rotate()
			out.mu.Unlock()
		}
	case rotateModeSize:
		if out.curSize+uint64(len(data)) > f.setting.rotateSize {
			out.mu.Lock()
			out.curIndex++
			atomic.SwapUint64(&out.curSize, 0)
			out.rotate()
			out.mu.Unlock()
		} else {
			atomic.AddUint64(&out.curSize, uint64(len(data)))
		}
	default:
	}
	out.fd.Write(data)
}

func (o *outer) rotate() {
	o.fd.Sync()
	o.fd.Close()
	o.genFile()
}

func (o *outer) genFile() {
	var (
		link     = ""
		fileName = ""
	)
	switch o.setting.rmode {
	case rotateModeSize:
		cur := nowFunc()
		for {
			fileName = fmt.Sprintf("%s.log.%04d%02d%02d%08d",
				o.setting.opt.prefix,
				cur.Year(), cur.Month(), cur.Day(),
				o.curIndex,
			)
			link = fmt.Sprintf("%s.log", o.setting.opt.prefix)
			if o.setting.seprated && o.level > INFO {
				fileName = fmt.Sprintf("%s.log.wf.%04d%02d%02d%08d",
					o.setting.opt.prefix,
					cur.Year(), cur.Month(), cur.Day(),
					o.curIndex,
				)
				link = fmt.Sprintf("%s.log.wf", o.setting.opt.prefix)
			}
			fileName = filepath.Join(o.setting.dir, fileName)
			finfo, nerr := os.Lstat(fileName)
			if nerr != nil {
				break
			}
			if finfo.Size() < int64(o.setting.rotateSize) {
				o.curSize = uint64(finfo.Size())
				break
			}
			o.curIndex++
		}
	case rotateModeHour:
		fileName = fmt.Sprintf("%s.log.%s", o.setting.opt.prefix, getCurHour(nowFunc()))
		link = fmt.Sprintf("%s.log", o.setting.opt.prefix)
		if o.setting.seprated && o.level > INFO {
			fileName = fmt.Sprintf("%s.log.wf.%s", o.setting.opt.prefix, getCurHour(nowFunc()))
			link = fmt.Sprintf("%s.log.wf", o.setting.opt.prefix)
		}
		fileName = filepath.Join(o.setting.dir, fileName)
	default:
		fileName = fmt.Sprintf("%s.log", o.setting.opt.prefix)
		link = fileName
		if o.setting.seprated && o.level > INFO {
			fileName = fmt.Sprintf("%s.log.wf", o.setting.opt.prefix)
			link = fmt.Sprintf("%s.log.wf", o.setting.opt.prefix)
		}
		fileName = filepath.Join(o.setting.dir, fileName)
	}
	fd, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openFile err:%v", err)
		return
	}
	link = filepath.Join(o.setting.dir, link)
	if fileName != link && !o.setting.disableLink {
		target, err := filepath.Abs(fileName)
		softLink, err := filepath.Abs(link)
		os.Remove(softLink)
		err = os.Symlink(target, softLink)
		if err != nil {
			fmt.Fprintf(os.Stderr, "create link err:%v\n", err)
		}
	}
	o.fd = fd
	o.autoClear()
}

func (o *outer) autoClear() {
	if !o.setting.autoClear {
		return
	}
	fs, err := getFilesByDir(o.setting.dir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "getFilesByDir[%s] err:%v\n", o.setting.dir, err)
	}
	for _, f := range fs {
		for i := o.setting.clearHours; i < o.setting.clearHours+o.setting.clearStep; i++ {
			t := time.Now().Add(time.Duration(-1*i) * time.Hour)
			fn := fmt.Sprintf("%s.log.%s",
				o.setting.opt.prefix,
				getCurHour(t),
			)
			wf := fmt.Sprintf("%s.log.wf.%s",
				o.setting.opt.prefix,
				getCurHour(t),
			)
			fn = filepath.Join(o.setting.dir, fn)
			wf = filepath.Join(o.setting.dir, wf)
			if strings.HasPrefix(f, fn) || strings.HasPrefix(f, wf) {
				err := os.Remove(f)
				if err != nil {
					fmt.Fprintf(os.Stderr, "clear log[%s] err:%v\n", f, err)
				}
			}
		}
	}
}

func getCurHour(cur time.Time) string {
	return fmt.Sprintf("%04d%02d%02d%02d", cur.Year(), cur.Month(), cur.Day(), cur.Hour())
}
