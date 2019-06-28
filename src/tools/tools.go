package tools

import (
	"crdt"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	PROTO_PRINT    = "[PROTO_SERVER"
	MAT_PRINT      = "[MAT"
	TM_PRINT       = "[TM"
	LOG_PRINT      = "[LOG"
	REPL_PRINT     = "[REPLICATOR"
	REMOTE_PRINT   = "[REMOTE_CONNECTION"
	PROTOLIB_PRINT = "[PROTO_LIB"

	WARNING = "[WARNING]"
	ERROR   = "[ERROR]"
	INFO    = "[INFO]"
	DEBUG   = "[DEBUG]"
)

var (
	/*
		disabledDebugs = map[string]struct{}{
			PROTO_PRINT:    {},
			MAT_PRINT:      {},
			TM_PRINT:       {},
			LOG_PRINT:      {},
			REPL_PRINT:     {},
			REMOTE_PRINT:   {},
			PROTOLIB_PRINT: {},
		}
	*/
	disabledDebugs    = map[string]struct{}{}
	allOutputDisabled = true
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
	if !allOutputDisabled {
		if _, has := disabledDebugs[src]; !has {
			fancyPrint(DEBUG, src, replicaID, msgs)
		}
	}
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is an info message
func FancyInfoPrint(src string, replicaID int64, msgs ...interface{}) {
	if !allOutputDisabled {
		fancyPrint(INFO, src, replicaID, msgs)
	}
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is a warning message
func FancyWarnPrint(src string, replicaID int64, msgs ...interface{}) {
	if !allOutputDisabled {
		fancyPrint(WARNING, src, replicaID, msgs)
	}
}

//Prints a msg to stdout with a prefix of who generated the message and the indication that it is an error message
func FancyErrPrint(src string, replicaID int64, msgs ...interface{}) {
	if !allOutputDisabled {
		fancyPrint(ERROR, src, replicaID, msgs)
	}
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

func StateToString(state crdt.State) (stateString string) {
	var sb strings.Builder
	switch typedState := state.(type) {
	//Set
	case crdt.SetAWValueState:
		sb.WriteRune('[')
		//TODO: As of now slice might be called multiple times. This shouldn't be here.
		sort.Slice(typedState.Elems, func(i, j int) bool { return typedState.Elems[i] < typedState.Elems[j] })
		for _, elem := range typedState.Elems {
			sb.WriteString(string(elem) + ", ")
		}
		sb.WriteRune(']')
	case crdt.SetAWLookupState:
		if typedState.HasElem {
			sb.WriteString("Element found.")
		} else {
			sb.WriteString("Element not found.")
		}
	//Counter
	case crdt.CounterState:
		sb.WriteString("Value: ")
		sb.WriteRune(typedState.Value)
	//TopK
	case crdt.TopKValueState:
		sb.WriteRune('[')
		//TODO: As of now slice might be called multiple times. This shouldn't be here.
		sort.Slice(typedState.Scores, func(i, j int) bool { return typedState.Scores[i].Id < typedState.Scores[j].Id })
		for i, score := range typedState.Scores {
			sb.WriteString(fmt.Sprintf("%d: (%d, %d), ", i+1, score.Id, score.Score))
		}
		sb.WriteRune(']')
		/*
			type SetAWValueState struct {
				Elems []Element
			}

			type SetAWLookupState struct {
				hasElem bool
			}
			type CounterState struct {
				Value int32
			}
			type TopKValueState struct {
				Scores []TopKScore
		*/
	}
	return sb.String()
}
