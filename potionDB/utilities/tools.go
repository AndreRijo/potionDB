package utilities

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"

	"potionDB/crdt/crdt"
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
	disabledDebugs = map[string]struct{}{
		PROTO_PRINT:  {},
		MAT_PRINT:    {},
		TM_PRINT:     {},
		LOG_PRINT:    {},
		REPL_PRINT:   {},
		REMOTE_PRINT: {},
		//PROTOLIB_PRINT: {},
	}
	disabledInfos = map[string]struct{}{
		REMOTE_PRINT: {},
		REPL_PRINT:   {},
	}
	disabledWarnings = map[string]struct{}{
		MAT_PRINT: {},
	}

	//disabledDebugs    = map[string]struct{}{}
	//allOutputDisabled = true
	allOutputDisabled = false
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

// Prints a msg to stdout with a prefix of who generated the message and the indication that it is a debug message
func FancyDebugPrint(src string, replicaID int16, msgs ...interface{}) {
	if !allOutputDisabled {
		if _, has := disabledDebugs[src]; !has {
			fancyPrint(DEBUG, src, replicaID, msgs)
		}
	}
}

// Prints a msg to stdout with a prefix of who generated the message and the indication that it is an info message
func FancyInfoPrint(src string, replicaID int16, msgs ...interface{}) {
	if !allOutputDisabled {
		if _, has := disabledInfos[src]; !has {
			fancyPrint(INFO, src, replicaID, msgs)
		}
	}
}

// Prints a msg to stdout with a prefix of who generated the message and the indication that it is a warning message
func FancyWarnPrint(src string, replicaID int16, msgs ...interface{}) {
	if !allOutputDisabled {
		if _, has := disabledWarnings[src]; !has {
			fancyPrint(WARNING, src, replicaID, msgs)
		}
	}
}

// Prints a msg to stdout with a prefix of who generated the message and the indication that it is an error message
func FancyErrPrint(src string, replicaID int16, msgs ...interface{}) {
	//if !allOutputDisabled {
	fancyPrint(ERROR, src, replicaID, msgs)
	//}
}

func fancyPrint(typeMsg string, src string, replicaID int16, msgs []interface{}) {
	var builder strings.Builder
	builder.WriteString(src)
	builder.WriteString(" ")
	builder.WriteString(strconv.FormatInt(int64(replicaID), 10))
	builder.WriteString("]")
	builder.WriteString(typeMsg)
	fmt.Println(&builder, msgs)
}

func StateToString(state crdt.State) (stateString string) {
	var sb strings.Builder
	switch typedState := state.(type) {
	//Counter
	case crdt.CounterState:
		sb.WriteString("Value: ")
		sb.WriteString(fmt.Sprint(typedState))

	//Register
	case crdt.RegisterState:
		sb.WriteString("Value: ")
		sb.WriteString(fmt.Sprint(typedState.Value))

	//Set
	case crdt.SetAWValueState:
		//TODO: Test printing using fmt.Sprintln
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

	//Map
	case crdt.MapEntryState:
		sb.WriteString(fmt.Sprintln(typedState.Values))
	case crdt.MapGetValueState:
		sb.WriteString("Value: ")
		sb.WriteString(fmt.Sprint(typedState.Value))
	case crdt.MapHasKeyState:
		sb.WriteString("Key found: ")
		sb.WriteString(fmt.Sprint(typedState.HasKey))
	case crdt.MapKeysState:
		sort.Slice(typedState.Keys, func(i, j int) bool { return typedState.Keys[i] < typedState.Keys[j] })
		sb.WriteString(fmt.Sprintln(typedState.Keys))

	//EmbMap
	case crdt.EmbMapEntryState:
		keys := make([]string, len(typedState.States))
		i := 0
		for key := range typedState.States {
			keys[i] = key
			i++
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		sb.WriteRune('{')
		for _, key := range keys {
			sb.WriteString(key)
			sb.WriteRune(':')
			sb.WriteString(StateToString(typedState.States[key]))
			sb.WriteString(", ")
		}
		sb.WriteRune('}')
	case crdt.EmbMapGetValueState:
		sb.WriteString("Value: ")
		sb.WriteString(StateToString(typedState.State))
	case crdt.EmbMapHasKeyState:
		sb.WriteString("Key found: ")
		sb.WriteString(fmt.Sprint(typedState.HasKey))
	case crdt.EmbMapKeysState:
		sort.Slice(typedState.Keys, func(i, j int) bool { return typedState.Keys[i] < typedState.Keys[j] })
		sb.WriteString(fmt.Sprintln(typedState.Keys))

	//TopK
	case crdt.TopKValueState:
		sb.WriteRune('[')
		//TODO: As of now slice might be called multiple times. This shouldn't be here.
		sort.Slice(typedState.Scores, func(i, j int) bool { return typedState.Scores[i].Id < typedState.Scores[j].Id })
		for i, score := range typedState.Scores {
			sb.WriteString(fmt.Sprintf("%d: (%d, %d), ", i+1, score.Id, score.Score))
		}
		sb.WriteRune(']')

	//Avg
	case crdt.AvgState:
		sb.WriteString(fmt.Sprint(typedState.Value))
	//MaxMin
	case crdt.MaxMinState:
		sb.WriteString(fmt.Sprint(typedState.Value))
	}

	return sb.String()
}

// Note: Use with care as this uses reflection!
func GetFunctionName(fun interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(fun).Pointer()).Name()
}

func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}
