package dbclient

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
)

var servicePort string

// func checker(id int, ip, path, value string, c chan int) {
// 	for {
// 		body, err := httpGet("localhost", ip, path)
// 		if strings.Contains(string(body), "could not get") {
// 			continue
// 		}
// 		fmt.Fprintf(os.Stderr, "%d ret is: %s", id, string(body))
// 		if err == nil && strings.Contains(string(body), value) {
// 			c <- id
// 			return
// 		}
// 		// time.Sleep(100 * time.Millisecond)
// 	}
// }
//
func httpGetFatalError(ip, port, path string) {
	// logrus.Infof("httpGetFatalError %s:%s%s", ip, port, path)
	body, err := httpGet(ip, port, path)
	if err != nil || !strings.Contains(string(body), "OK") {
		log.Fatalf("Write error %s", err)
	}
}

func httpGet(ip, port, path string) ([]byte, error) {
	resp, err := http.Get("http://" + ip + ":" + port + path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "there was an error: %s\n", err)
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	return body, err
}

func joinCluster(ip, port string, members []string, doneCh chan error) {
	body, err := httpGet(ip, port, "/join?members="+strings.Join(members, ","))

	if err != nil || !strings.Contains(string(body), "OK") {
		logrus.Errorf("joinNetwork %s there was an error: %s %s\n", ip, err, body)
	}
	doneCh <- err
}

func joinNetwork(ip, port, network string, doneCh chan error) {
	body, err := httpGet(ip, port, "/joinnetwork?nid="+network)

	if err != nil || !strings.Contains(string(body), "OK") {
		logrus.Errorf("joinNetwork %s there was an error: %s\n", ip, err)
	}
	if doneCh != nil {
		doneCh <- err
	}
}

func leaveNetwork(ip, port, network string, doneCh chan error) {
	body, err := httpGet(ip, port, "/leavenetwork?nid="+network)

	if err != nil || !strings.Contains(string(body), "OK") {
		logrus.Errorf("leaveNetwork %s there was an error: %s\n", ip, err)
	}
	if doneCh != nil {
		doneCh <- err
	}
}

func writeTableKey(ip, port, networkName, tableName, key string) {
	createPath := "/createentry?nid=" + networkName + "&tname=" + tableName + "&value=v&key="
	httpGetFatalError(ip, port, createPath+key)
}

func deleteTableKey(ip, port, networkName, tableName, key string) {
	deletePath := "/deleteentry?nid=" + networkName + "&tname=" + tableName + "&key="
	httpGetFatalError(ip, port, deletePath+key)
}

func clusterPeersNumber(ip, port string, doneCh chan int) {
	body, err := httpGet(ip, port, "/clusterpeers")

	if err != nil {
		logrus.Errorf("clusterPeers %s there was an error: %s\n", ip, err)
		doneCh <- -1
		return
	}
	peersRegexp := regexp.MustCompile(`Total peers: ([0-9]+)`)
	peersNum, _ := strconv.Atoi(peersRegexp.FindStringSubmatch(string(body))[1])

	doneCh <- peersNum
}

func networkPeersNumber(ip, port, networkName string, doneCh chan int) {
	body, err := httpGet(ip, port, "/networkpeers?nid="+networkName)

	if err != nil {
		logrus.Errorf("networkPeersNumber %s there was an error: %s\n", ip, err)
		doneCh <- -1
		return
	}
	peersRegexp := regexp.MustCompile(`Total peers: ([0-9]+)`)
	peersNum, _ := strconv.Atoi(peersRegexp.FindStringSubmatch(string(body))[1])

	doneCh <- peersNum
}

func tableEntriesNumber(ip, port, networkName, tableName string, doneCh chan int) {
	body, err := httpGet(ip, port, "/gettable?nid="+networkName+"&tname="+tableName)

	if err != nil {
		logrus.Errorf("tableEntriesNumber %s there was an error: %s\n", ip, err)
		doneCh <- -1
		return
	}
	elementsRegexp := regexp.MustCompile(`total elements: ([0-9]+)`)
	entriesNum, _ := strconv.Atoi(elementsRegexp.FindStringSubmatch(string(body))[1])
	logrus.Infof("Node %s has %d entries", ip, entriesNum)
	doneCh <- entriesNum
}

func writeDeleteUniqueKeys(ctx context.Context, ip, port, networkName, tableName, key string, doneCh chan int) {
	for x := 0; ; x++ {
		select {
		case <-ctx.Done():
			logrus.Infof("Exiting after having written %s keys", strconv.Itoa(x))
			doneCh <- x
			return
		default:
			k := key + strconv.Itoa(x)
			// write key
			writeTableKey(ip, port, networkName, tableName, k)
			// give time to send out key writes
			time.Sleep(100 * time.Millisecond)
			// delete key
			deleteTableKey(ip, port, networkName, tableName, k)
		}
	}
}

func writeDeleteLeaveJoin(ctx context.Context, ip, port, networkName, tableName, key string, doneCh chan int) {
	for x := 0; ; x++ {
		select {
		case <-ctx.Done():
			logrus.Infof("Exiting after having written %s keys", strconv.Itoa(x))
			doneCh <- x
			return
		default:
			k := key + strconv.Itoa(x)
			// write key
			writeTableKey(ip, port, networkName, tableName, k)
			time.Sleep(100 * time.Millisecond)
			time.Sleep(100 * time.Millisecond)
			// delete key
			deleteTableKey(ip, port, networkName, tableName, k)
			// give some time
			time.Sleep(100 * time.Millisecond)
			// leave network
			leaveNetwork(ip, port, networkName, nil)
			// join network
			joinNetwork(ip, port, networkName, nil)
		}
	}
}

//
// func reproIssue(writerList []string, keyBase string) {
// 	workers := len(writerList)
// 	doneCh := make(chan int)
// 	ctx, cancel := context.WithCancel(context.Background())
//
// 	// start the write in parallel
// 	for _, w := range writerList {
// 		key := keyBase + w
// 		fmt.Fprintf(os.Stderr, "Spawn worker: %s\n", w)
// 		// time.Sleep(300 * time.Millisecond)
// 		go doWriteDeleteLeaveJoinSingle(ctx, w, key, doneCh)
// 	}
//
// 	// // start only one writer
// 	// writer := writerList[0]
// 	// key := keyBase + writer
// 	// fmt.Fprintf(os.Stderr, "Spawn worker: %s\n", writer)
// 	// go doWriteDeleteLeaveJoinSingle(ctx, writer, key, doneCh)
//
// 	// wait for the worker to finish
// 	for workers > 0 {
// 		fmt.Fprintf(os.Stderr, "Remains: %d workers\n", workers)
// 		<-doneCh
// 		workers--
// 	}
// 	cancel()
//
// 	// Stop when stable
// 	stableResult := 3
// 	start := time.Now().UnixNano()
// 	for {
// 		time.Sleep(2 * time.Second)
// 		fmt.Fprintf(os.Stderr, "Checking node tables\n")
// 		var equal int
// 		var prev []byte
// 		for i, w := range writerList {
// 			path := "/gettable?nid=test&tname=table_name"
// 			body, err := httpGet("localhost", string(w), path)
// 			if err != nil {
// 				fmt.Fprintf(os.Stderr, "there was an error: %s\n", err)
// 				return
// 			}
// 			_, line, _ := bufio.ScanLines(body, false)
// 			fmt.Fprintf(os.Stderr, "%s writer ret: %s\n", w, line)
//
// 			if i > 0 {
// 				if bytes.Equal(prev, body) {
// 					equal++
// 				} else {
// 					equal = 0
// 					stableResult = 3
// 				}
// 			}
// 			prev = body
// 			if equal == len(writerList)-1 {
// 				stableResult--
// 				if stableResult == 0 {
// 					opTime := time.Now().UnixNano() - start
// 					fmt.Fprintf(os.Stderr, "the output is stable after: %dms\n", opTime/1000000)
// 					return
// 				}
// 			}
// 		}
// 	}
//
// }

// func main() {
// 	if len(os.Args) < 3 {
// 		log.Fatal("You need to specify the port and path")
// 	}
// 	operation := os.Args[1]
// 	nodes := strings.Split(os.Args[2], ",")
// 	key := "testKey-"
// 	if len(os.Args) > 3 {
// 		key = os.Args[3]
// 	}
//
// 	switch operation {
// 	case "write-propagate":
// 		start := time.Now().UnixNano()
// 		writeAndPropagate(nodes[0], "/createentry?nid=test&tname=table_name&value=v&key="+key, key, nodes)
// 		opTime := time.Now().UnixNano() - start
// 		fmt.Fprintf(os.Stderr, "operation took: %dms\n", opTime/1000000)
// 	case "write-delete":
// 		writeAndDelete(nodes, "testKey-")
// 	case "write-delete-leave-join":
// 		writeAndDeleteLeaveJoin(nodes, "testKey-")
// 	case "repro-issue":
// 		reproIssue(nodes, "issueKey-")
// 	default:
// 		log.Fatal("Operations: write-propagate, write-delete")
// 	}
// }

func ready(ip, port string, doneCh chan error) {
	for {
		body, err := httpGet(ip, port, "/ready")
		if err != nil || !strings.Contains(string(body), "OK") {
			time.Sleep(500 * time.Millisecond)
			continue
		}
		// success
		break
	}
	// notify the completion
	doneCh <- nil
}

// ready
func doReady(ips []string) {
	doneCh := make(chan error, len(ips))
	// check all the nodes
	for _, ip := range ips {
		go ready(ip, servicePort, doneCh)
	}
	// wait for the readiness of all nodes
	for i := len(ips); i > 0; i-- {
		<-doneCh
	}
	close(doneCh)
}

// join
func doJoin(ips []string) {
	doneCh := make(chan error, len(ips))
	// check all the nodes
	for i, ip := range ips {
		members := append([]string(nil), ips[:i]...)
		members = append(members, ips[i+1:]...)
		go joinCluster(ip, servicePort, members, doneCh)
	}
	// wait for the readiness of all nodes
	for i := len(ips); i > 0; i-- {
		err := <-doneCh
		if err != nil {
			log.Fatalf("The join failed with error:%s", err)
		}
	}
	close(doneCh)
}

// cluster-peers expectedNumberPeers
func doClusterPeers(ips []string, args []string) {
	doneCh := make(chan int, len(ips))
	expectedPeers, _ := strconv.Atoi(args[0])
	// check all the nodes
	for _, ip := range ips {
		go clusterPeersNumber(ip, servicePort, doneCh)
	}
	// wait for the readiness of all nodes
	for i := len(ips); i > 0; i-- {
		peers := <-doneCh
		if peers != expectedPeers {
			log.Fatalf("Expected peers missmatch %d != %d", expectedPeers, peers)
		}
	}
	close(doneCh)
}

// join-network networkName
func doJoinNetwork(ips []string, args []string) {
	doneCh := make(chan error, len(ips))
	// check all the nodes
	for _, ip := range ips {
		go joinNetwork(ip, servicePort, args[0], doneCh)
	}
	// wait for the readiness of all nodes
	for i := len(ips); i > 0; i-- {
		<-doneCh
	}
	close(doneCh)
}

// leave-network networkName
func doLeaveNetwork(ips []string, args []string) {
	doneCh := make(chan error, len(ips))
	// check all the nodes
	for _, ip := range ips {
		go leaveNetwork(ip, servicePort, args[0], doneCh)
	}
	// wait for the readiness of all nodes
	for i := len(ips); i > 0; i-- {
		<-doneCh
	}
	close(doneCh)
}

// cluster-peers networkName expectedNumberPeers maxRetry
func doNetworkPeers(ips []string, args []string) {
	doneCh := make(chan int, len(ips))
	networkName := args[0]
	expectedPeers, _ := strconv.Atoi(args[1])
	maxRetry, _ := strconv.Atoi(args[2])
	for retry := 0; retry < maxRetry; retry++ {
		// check all the nodes
		for _, ip := range ips {
			go networkPeersNumber(ip, servicePort, networkName, doneCh)
		}
		// wait for the readiness of all nodes
		for i := len(ips); i > 0; i-- {
			peers := <-doneCh
			if peers != expectedPeers {
				if retry == 2 {
					log.Fatalf("Expected peers missmatch %d != %d", expectedPeers, peers)
				} else {
					logrus.Warnf("Expected peers missmatch %d != %d", expectedPeers, peers)
				}
				time.Sleep(1 * time.Second)
			}
		}
	}
	close(doneCh)
}

// write-delete-unique-keys networkName tableName numParallelWriters writeTimeSec
func doWriteDeleteUniqueKeys(ips []string, args []string) {
	networkName := args[0]
	tableName := args[1]
	parallelWriters, _ := strconv.Atoi(args[2])
	writeTimeSec, _ := strconv.Atoi(args[3])

	// Start parallel writers that will create and delete unique keys
	doneCh := make(chan int, parallelWriters)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(writeTimeSec)*time.Second)
	for i := 0; i < parallelWriters; i++ {
		key := "key-" + strconv.Itoa(i) + "-"
		logrus.Infof("Spawn worker: %d on IP:%s", i, ips[i])
		go writeDeleteUniqueKeys(ctx, ips[i], servicePort, networkName, tableName, key, doneCh)
	}

	var totalKeys int
	for i := 0; i < parallelWriters; i++ {
		logrus.Infof("Waiting for %d workers", parallelWriters-i)
		keysWritten := <-doneCh
		totalKeys += keysWritten
		if keysWritten == 0 {
			log.Fatalf("The worker did not write any key %d == 0", keysWritten)
		}
	}
	close(doneCh)
	cancel()
	logrus.Infof("Written a total of %d keys on the cluster", totalKeys)

	// check table entries for 2 minutes
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	startTime := time.Now().UnixNano()
	var successTime int64

	time.Sleep(30 * time.Second)
	// Loop for 2 minutes to guartee that the result is stable
	for {
		select {
		case <-ctx.Done():
			cancel()
			// Validate test success, if the time is set means that all the tables are empty
			if successTime != 0 {
				logrus.Infof("Timer expired, the cluster converged in %d msec", time.Duration(successTime-startTime)/time.Millisecond)
				return
			}
			log.Fatal("Test failed, there is still entries in the tables of the nodes")
		default:
			logrus.Infof("Checking node tables")
			doneCh = make(chan int, len(ips))
			for _, ip := range ips {
				go tableEntriesNumber(ip, servicePort, networkName, tableName, doneCh)
			}

			nodesWithZeroEntries := 0
			for i := len(ips); i > 0; i-- {
				tableEntries := <-doneCh
				if tableEntries == 0 {
					nodesWithZeroEntries++
				}
			}
			close(doneCh)
			if nodesWithZeroEntries == len(ips) {
				if successTime == 0 {
					successTime = time.Now().UnixNano()
					logrus.Infof("Success after %d msec", time.Duration(successTime-startTime)/time.Millisecond)
				}
			} else {
				successTime = 0
			}
			time.Sleep(10 * time.Second)
		}
	}
}

// write-delete-leave-join networkName tableName numParallelWriters writeTimeSec
func doWriteDeleteLeaveJoin(ips []string, args []string) {
	networkName := args[0]
	tableName := args[1]
	parallelWriters, _ := strconv.Atoi(args[2])
	writeTimeSec, _ := strconv.Atoi(args[3])

	// Start parallel writers that will create and delete unique keys
	doneCh := make(chan int, parallelWriters)
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(writeTimeSec)*time.Second)
	for i := 0; i < parallelWriters; i++ {
		key := "key-" + strconv.Itoa(i) + "-"
		logrus.Infof("Spawn worker: %d on IP:%s", i, ips[i])
		go writeDeleteLeaveJoin(ctx, ips[i], servicePort, networkName, tableName, key, doneCh)
	}

	var totalKeys int
	for i := 0; i < parallelWriters; i++ {
		logrus.Infof("Waiting for %d workers", parallelWriters-i)
		keysWritten := <-doneCh
		totalKeys += keysWritten
		if keysWritten == 0 {
			log.Fatalf("The worker did not write any key %d == 0", keysWritten)
		}
	}
	close(doneCh)
	cancel()
	logrus.Infof("Written a total of %d keys on the cluster", totalKeys)

	// check table entries for 2 minutes
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Minute)
	startTime := time.Now().UnixNano()
	var successTime int64

	time.Sleep(30 * time.Second)
	// Loop for 2 minutes to guartee that the result is stable
	for {
		select {
		case <-ctx.Done():
			cancel()
			// Validate test success, if the time is set means that all the tables are empty
			if successTime != 0 {
				logrus.Infof("Timer expired, the cluster converged in %d msec", time.Duration(successTime-startTime)/time.Millisecond)
				return
			}
			log.Fatal("Test failed, there is still entries in the tables of the nodes")
		default:
			logrus.Infof("Checking node tables")
			doneCh = make(chan int, len(ips))
			for _, ip := range ips {
				go tableEntriesNumber(ip, servicePort, networkName, tableName, doneCh)
			}

			nodesWithZeroEntries := 0
			for i := len(ips); i > 0; i-- {
				tableEntries := <-doneCh
				if tableEntries == 0 {
					nodesWithZeroEntries++
				}
			}
			close(doneCh)
			if nodesWithZeroEntries == len(ips) {
				if successTime == 0 {
					successTime = time.Now().UnixNano()
					logrus.Infof("Success after %d msec", time.Duration(successTime-startTime)/time.Millisecond)
				}
			} else {
				successTime = 0
			}
			time.Sleep(10 * time.Second)
		}
	}
}

var cmdArgChec = map[string]int{
	"debug":                    0,
	"fail":                     0,
	"ready":                    2,
	"join":                     2,
	"leave":                    2,
	"join-network":             3,
	"leave-network":            3,
	"cluster-peers":            3,
	"write-delete-unique-keys": 4,
}

// Client is a client
func Client(args []string) {
	logrus.Infof("[CLIENT] Starting with arguments %v", args)
	command := args[0]

	if len(args) < cmdArgChec[command] {
		log.Fatalf("Command %s requires %d arguments, aborting...", command, cmdArgChec[command])
	}

	switch command {
	case "debug":
		time.Sleep(1 * time.Hour)
		os.Exit(0)
	case "fail":
		log.Fatalf("Test error condition with message: error error error")
	}

	serviceName := args[1]
	ips, _ := net.LookupHost("tasks." + serviceName)
	logrus.Infof("got the ips %v", ips)
	if len(ips) == 0 {
		log.Fatalf("Cannot resolve any IP for the service tasks.%s", serviceName)
	}
	servicePort = args[2]
	commandArgs := args[3:]
	logrus.Infof("Executing %s with args:%v", command, commandArgs)
	switch command {
	case "ready":
		doReady(ips)
	case "join":
		doJoin(ips)
	case "leave":

	case "cluster-peers":
		// cluster-peers
		doClusterPeers(ips, commandArgs)

	case "join-network":
		// join-network networkName
		doJoinNetwork(ips, commandArgs)
	case "leave-network":
		// leave-network networkName
		doLeaveNetwork(ips, commandArgs)
	case "network-peers":
		// network-peers networkName maxRetry
		doNetworkPeers(ips, commandArgs)

	case "write-delete-unique-keys":
		// write-delete-unique-keys networkName tableName numParallelWriters writeTimeSec
		doWriteDeleteUniqueKeys(ips, commandArgs)
	case "write-delete-leave-join":
		// write-delete-leave-join networkName tableName numParallelWriters writeTimeSec
		doWriteDeleteLeaveJoin(ips, commandArgs)
	default:
		log.Fatalf("Command %s not recognized", command)
	}
}
