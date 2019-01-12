package logger

// NewLoggerWithOption ...
func NewLoggerWithOption(dir string, opts ...Option) Logger {
	fs := &fileSetting{
		dir:         dir,
		enable:      true,
		maxLevel:    TRACE,
		format:      defaultFormat,
		async:       true,
		rmode:       rotateModeHour,
		seprated:    true,
		disableLink: true,
	}
	for _, opt := range opts {
		opt(fs)
	}
	if fs.callDepth == 0 {
		fs.callDepth = 3
	}
	return newFileLogWithSetting(fs)
}

// Option ...
type Option func(fs *fileSetting)

// SetFormat 设置日志格式
func SetFormat(format string) Option {
	return func(fs *fileSetting) {
		fs.format = format
	}
}

// SetPrefix 设置日志文件名前缀,如xxx,则日志为xxx.log
func SetPrefix(prefix string) Option {
	return func(fs *fileSetting) {
		if fs.opt == nil {
			fs.opt = &option{}
		}
		fs.opt.prefix = prefix
	}
}

// SetMaxLevel 设置日志级别
func SetMaxLevel(logLevel string) Option {
	return func(fs *fileSetting) {
		if fs.opt == nil {
			fs.opt = &option{}
		}
		fs.maxLevel = getLogLevel(logLevel)
	}
}

// SetAutoClear 自动清理日志
func SetAutoClear(autoClear bool) Option {
	return func(fs *fileSetting) {
		fs.autoClear = autoClear
	}
}

// SetAutoClearHours 清理keepHours小时以前的日志
func SetAutoClearHours(keepHours int) Option {
	return func(fs *fileSetting) {
		fs.clearHours = int32(keepHours)
	}
}

// SetRotateByHour 按小时切分
func SetRotateByHour() Option {
	return func(fs *fileSetting) {
		fs.rotateByHour = true
	}
}

// SetSeprate info日志和wf日志分开
func SetSeprate(seprated bool) Option {
	return func(fs *fileSetting) {
		fs.seprated = seprated
	}
}

// SetLinkable 是否设置软连接
func SetLinkable(useSoftLink bool) Option {
	return func(fs *fileSetting) {
		fs.disableLink = !useSoftLink
	}
}

// SetFileCallerDepth 设置当前的depth
func SetFileCallerDepth(dep int) Option {
	return func(fs *fileSetting) {
		fs.callDepth = dep
	}
}
