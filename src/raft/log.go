package raft

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

var writer *bufio.Writer
var mu sync.Mutex

func init() {
	f, err := os.Create("log")
	if err != nil {
		panic("create log file error")
	}
	writer = bufio.NewWriter(f)
}

func Info(me int, formatStr string, a ...any) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintf(writer, "[info][%v]", me)
	fmt.Fprintf(writer, formatStr, a...)
	fmt.Fprintf(writer, "\n")
}

func Warn(me int, formatStr string, a ...any) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintf(writer, "[warn][%v]", me)
	fmt.Fprintf(writer, formatStr, a...)
	fmt.Fprintf(writer, "\n")
}

func Error(me int, formatStr string, a ...any) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintf(writer, "[error][%v]", me)
	fmt.Fprintf(writer, formatStr, a...)
	fmt.Fprintf(writer, "\n")
}

func Fatal(me int, formatStr string, a ...any) {
	mu.Lock()
	defer mu.Unlock()
	fmt.Fprintf(writer, "[fatal][%v]", me)
	fmt.Fprintf(writer, formatStr, a...)
	fmt.Fprintf(writer, "\n")
}
