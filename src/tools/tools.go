package tools

import (
	"fmt"
	"net"
	"os"
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
