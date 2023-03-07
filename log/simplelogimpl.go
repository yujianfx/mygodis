package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type Settings struct {
	Path       string `yaml:"path"`
	Name       string `yaml:"name"`
	Ext        string `yaml:"ext"`
	TimeFormat string `yaml:"time-format"`
}

var (
	logFile            *os.File
	defaultPrefix      = ""
	defaultCallerDepth = 2 //stackFrame num
	loggerImpl         *log.Logger
	mu                 sync.Mutex
	logPrefix          = ""
	levelFlags         = []string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}
)

type logLevel int

const (
	DEBUG logLevel = iota
	INFO
	WARNING
	ERROR
	FATAL
)

const flags = log.LstdFlags

func init() {
	loggerImpl = log.New(os.Stdout, defaultPrefix, flags)
}
func Setup(settings *Settings) {
	var err error
	dir := settings.Path
	fileName := fmt.Sprintf("%s-%s.%s",
		settings.Name,
		time.Now().Format(settings.TimeFormat),
		settings.Ext)

	logFile, err = mustOpen(fileName, dir)
	if err != nil {
		log.Fatalf("logging.Setup err: %s", err)
	}
	multiWriter := io.MultiWriter(os.Stdout, logFile)
	loggerImpl = log.New(multiWriter, defaultPrefix, flags)
}

func mustOpen(fileName string, dir string) (*os.File, error) {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create dir: %s", err)
		}
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("permission denied dir: %s", dir)
	}
	fullName := filepath.Join(dir, fileName)
	file, err := os.OpenFile(fullName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %s", err)
	}
	return file, nil
}
func setPrefix(level logLevel) {
	_, file, line, ok := runtime.Caller(defaultCallerDepth)
	if ok {
		logPrefix = fmt.Sprintf("[%s][%s:%d] ", levelFlags[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("[%s] ", levelFlags[level])
	}

	loggerImpl.SetPrefix(logPrefix)
}

// Debug prints debug log
func Debug(v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(DEBUG)
	loggerImpl.Println(v...)
}

// Info prints normal log
func Info(v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(INFO)
	loggerImpl.Println(v...)
}

// Warn prints warning log
func Warn(v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(WARNING)
	loggerImpl.Println(v...)
}

// Error prints error log
func Error(v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(ERROR)
	loggerImpl.Println(v...)
}

func Errorf(format string, v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(ERROR)
	loggerImpl.Println(fmt.Sprintf(format, v...))
}

// Fatal prints error log then stop the program
func Fatal(v ...any) {
	mu.Lock()
	defer mu.Unlock()
	setPrefix(FATAL)
	loggerImpl.Fatalln(v...)
}
