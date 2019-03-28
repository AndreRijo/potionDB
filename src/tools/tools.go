package tools

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	PROTO_PRINT  = "[PROTO_SERVER"
	MAT_PRINT    = "[MAT"
	TM_PRINT     = "[TM"
	LOG_PRINT    = "[LOG"
	REPL_PRINT   = "[REPLICATOR"
	REMOTE_PRINT = "[REMOTE_CONNECTION"

	WARNING = "[WARNING]"
	ERROR   = "[ERROR]"
	INFO    = "[INFO]"
	DEBUG   = "[DEBUG]"
)

var (
	disabledDebugs = map[string]struct{}{PROTO_PRINT: {}, MAT_PRINT: {}, TM_PRINT: {}, LOG_PRINT: {}, REPL_PRINT: {}, REMOTE_PRINT: {}}
)

func CheckErr(msg string, err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, msg, err)
		os.Exit(1)
	}
}

func ReadFromNetwork(nBytes int, conn net.Conn) (sizeBuf []byte) {
	sizeBuf = make([]byte, nBytes)
	for nRead := 0; nRead < nBytes; {
		n, err := conn.Read(sizeBuf[nRead:])
		CheckErr(NETWORK_READ_ERROR, err)
		nRead += n
	}
	return
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is a debug message
func FancyDebugPrint(src string, replicaID int64, msgs ...interface{}) {
	if _, has := disabledDebugs[src]; !has {
		fancyPrint(DEBUG, src, replicaID, msgs)
	}
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is an info message
func FancyInfoPrint(src string, replicaID int64, msgs ...interface{}) {
	fancyPrint(INFO, src, replicaID, msgs)
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is a warning message
func FancyWarnPrint(src string, replicaID int64, msgs ...interface{}) {
	fancyPrint(WARNING, src, replicaID, msgs)
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is an error message
func FancyErrPrint(src string, replicaID int64, msgs ...interface{}) {
	fancyPrint(ERROR, src, replicaID, msgs)
}

func fancyPrint(typeMsg string, src string, replicaID int64, msgs []interface{}) {
	var builder strings.Builder
	builder.WriteString(src)
	builder.WriteString(" ")
	builder.WriteString(strconv.FormatInt(replicaID, 10))
	builder.WriteString("]")
	builder.WriteString(typeMsg)
	fmt.Println(&builder, msgs)
}
