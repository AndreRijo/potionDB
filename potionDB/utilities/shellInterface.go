//A group of tools to execute shell commands

package utilities

import (
	"fmt"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

const (
	//TC_BASE_COMMAND   = "tc qdisc add dev eth0"
	//TC_FILTER_COMMAND = "tc filter add dev eth0"
	TC_BASE_PRIOMAP = "0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0"
	//eth0 if using docker network; some other name if using net=host
	ETHERNET_NAME = "eth0"
	//ETHERNET_NAME = "enp24s0f0"
)

// Assumes Latencies[MyIpPos] = 0
type TcInfo struct {
	Ips       []string
	MyIpPos   int
	Latencies []float64
}

func MakeTcInfo(ips []string, myIpPos int, latencies []string) TcInfo {
	floatLatencies := make([]float64, len(latencies))
	for i, str := range latencies {
		floatLatencies[i], _ = strconv.ParseFloat(str, 64)
	}
	return TcInfo{Ips: ips, MyIpPos: myIpPos, Latencies: floatLatencies}
}

func (tc TcInfo) FireTcCommands() {

	//Sleep for a while to give time for the other replicas to start before doing ping
	time.Sleep(1000 * time.Millisecond)

	solvedIps := make([]string, len(tc.Ips))
	for i, ip := range tc.Ips {
		solvIp, err := net.LookupIP(ip)
		if err != nil {
			fmt.Println("[TC]Error solving addresses - tc may fail to work.")
			solvedIps = tc.Ips
			break
		} else {
			solvedIps[i] = solvIp[0].String()
		}
	}

	tc.Ips = solvedIps

	fmt.Printf("%v\n", tc)

	//fmt.Println("[TC]Firing TC commands")
	//Creates a prio qdisc with len(Ips) bands
	//startCmd := exec.Command("tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio",
	//"bands", strconv.Itoa(len(tc.Ips)), "priomap", TC_BASE_PRIOMAP)
	startCmd := exec.Command("tc", "qdisc", "add", "dev", ETHERNET_NAME, "root", "handle", "1:", "prio",
		"bands", strconv.Itoa(len(tc.Ips)), "priomap", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0",
		"0", "0", "0", "0", "0", "0")

	//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n",
	//"tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio", "bands", strconv.Itoa(len(tc.Ips)),
	//"priomap", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0", "0")

	netemCmds := make([]*exec.Cmd, len(tc.Latencies)-1)
	filterCmds := make([]*exec.Cmd, len(tc.Latencies)-1)
	testPings := make([]*exec.Cmd, len(tc.Latencies)-1)
	i := 0
	for i = tc.MyIpPos + 1; i < len(tc.Ips); i++ {
		//Latency rule
		netemCmds[i-1] = exec.Command("tc", "qdisc", "add", "dev", ETHERNET_NAME, "parent", "1:"+strconv.Itoa(i+1),
			"handle", strconv.Itoa(i+1)+"0:", "netem", "delay", strconv.FormatFloat(tc.Latencies[i], 'f', -1, 64)+"ms")
		//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s\n", "tc", "qdisc", "add", "dev", "eth0", "parent 1:"+strconv.Itoa(i+1),
		//"handle", strconv.Itoa(i+1)+"0:", "netem", "delay", strconv.FormatFloat(tc.Latencies[i], 'f', -1, 64)+"ms")
		//Associates latency rule to IP
		filterCmds[i-1] = exec.Command("tc", "filter", "add", "dev", ETHERNET_NAME, "protocol", "ip", "parent",
			"1:0", "prio", "1", "u32", "match", "ip", "dst", tc.Ips[i], "flowid", "1:"+strconv.Itoa(i+1))
		//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", "tc", "filter", "add", "dev", "eth0", "protocol", "ip", "parent",
		//"1:0", "prio", "1", "u32", "match", "ip", "dst", tc.Ips[i], "flowid", "1:"+strconv.Itoa(i+1))
		testPings[i-1] = exec.Command("ping", "-c", "1", tc.Ips[i])
	}
	for i = 0; i < tc.MyIpPos; i++ {
		netemCmds[i] = exec.Command("tc", "qdisc", "add", "dev", ETHERNET_NAME, "parent", "1:"+strconv.Itoa(i+1),
			"handle", strconv.Itoa(i+1)+"0:", "netem", "delay", strconv.FormatFloat(tc.Latencies[i], 'f', -1, 64)+"ms")
		//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s\n", "tc", "qdisc", "add", "dev", "eth0", "parent 1:"+strconv.Itoa(i+1),
		//"handle", strconv.Itoa(i+1)+"0:", "netem", "delay", strconv.FormatFloat(tc.Latencies[i], 'f', -1, 64)+"ms")
		filterCmds[i] = exec.Command("tc", "filter", "add", "dev", ETHERNET_NAME, "protocol", "ip", "parent",
			"1:0", "prio", "1", "u32", "match", "ip", "dst", tc.Ips[i], "flowid", "1:"+strconv.Itoa(i+1))
		//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", "tc", "filter", "add", "dev", "eth0", "protocol", "ip", "parent",
		//"1:0", "prio", "1", "u32", "match", "ip", "dst", tc.Ips[i], "flowid", "1:"+strconv.Itoa(i+1))
		testPings[i] = exec.Command("ping", "-c", "1", tc.Ips[i])
	}

	//Associates every other IP to the non-modified band
	catchAllCmd := exec.Command("tc", "filter", "add", "dev", ETHERNET_NAME, "protocol", "ip", "parent", "1:",
		"prio", "2", "u32", "match", "ip", "dst", "0.0.0.0/0", "flowid", "1:"+strconv.Itoa(tc.MyIpPos+1))
	//fmt.Printf("%s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s\n", "tc", "filter", "add", "dev", "eth0", "protocol", "ip", "parent", "1:",
	//"prio", "2", "u32", "match", "ip", "dst", "0.0.0.0/0", "flowid", "1:"+strconv.Itoa(tc.MyIpPos+1))

	startCmd.Run()
	for i = 0; i < len(tc.Ips)-1; i++ {
		netemCmds[i].Run()
		filterCmds[i].Run()
	}
	catchAllCmd.Run()

	for i = 0; i < len(tc.Ips)-1; i++ {
		go func(cmd *exec.Cmd) {
			output, err := cmd.Output()
			strOutput := string(output)
			splitString := strings.Split(strOutput, "\n")
			last := splitString[len(splitString)-2]
			fmt.Printf("%s; Error: %v\n", last, err)
		}(testPings[i])
	}

	/*
		startMsg, _ := startCmd.CombinedOutput()
		netemMsgs, filterMsgs := make([][]byte, len(netemCmds)), make([][]byte, len(filterCmds))
		for i = 0; i < len(tc.Ips)-1; i++ {
			netemMsgs[i], _ = netemCmds[i].CombinedOutput()
			filterMsgs[i], _ = filterCmds[i].CombinedOutput()
		}
		catchAllMsg, _ := catchAllCmd.CombinedOutput()
		fmt.Println("[TC]All TC commands fired.")

		fmt.Println(string(startMsg))
		for i := 0; i < len(netemMsgs); i++ {
			fmt.Println(string(netemMsgs[i]))
			fmt.Println(string(filterMsgs[i]))
		}
		fmt.Println(string(catchAllMsg))
	*/
}
