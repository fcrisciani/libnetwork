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
	doneCh <- err
}

func leaveNetwork(ip, port, network string, doneCh chan error) {
	body, err := httpGet(ip, port, "/leavenetwork?nid="+network)

	if err != nil || !strings.Contains(string(body), "OK") {
		logrus.Errorf("leaveNetwork %s there was an error: %s\n", ip, err)
	}
	doneCh <- err
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

// func writeAndPropagate(writer, path, key string, waitForNodes []string) {
// 	writeKey(writer, path)
//
// 	nodes := len(waitForNodes)
// 	ch := make(chan int)
// 	for i, node := range waitForNodes {
// 		go checker(i, node, "/getentry?nid=test&tname=table_name&key="+key, "v", ch)
// 	}
//
// 	for {
// 		fmt.Fprintf(os.Stderr, "Missing %d nodes\n", nodes)
// 		id := <-ch
// 		fmt.Fprintf(os.Stderr, "%d done\n", id)
// 		nodes--
// 		if nodes == 0 {
// 			break
// 		}
// 	}
//
// }
//

func writeDeleteUniqueKeys(ctx context.Context, ip, port string, key, networkName, tableName string, doneCh chan int) {
	createPath := "/createentry?nid=" + networkName + "&tname=" + tableName + "&value=v&key="
	deletePath := "/deleteentry?nid=" + networkName + "&tname=" + tableName + "&key="
	for x := 0; ; x++ {
		select {
		case <-ctx.Done():
			logrus.Infof("Exiting after having written %s keys", strconv.Itoa(x))
			doneCh <- x
			return
		default:
			k := key + strconv.Itoa(x)
			// write key
			httpGetFatalError(ip, port, createPath+k)
			// give time to send out key writes
			time.Sleep(100 * time.Millisecond)
			// delete key
			httpGetFatalError(ip, port, deletePath+k)
		}
	}
}

// func doWriteDeleteLeaveJoin(ctx context.Context, port, key string, doneCh chan int) {
// 	x := 0
// 	createPath := "/createentry?nid=test&tname=table_name&value=v&key="
// 	deletePath := "/deleteentry?nid=test&tname=table_name&key="
//
// 	fmt.Fprintf(os.Stderr, "%s Started\n", key)
//
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			fmt.Fprintf(os.Stderr, "Exiting after having written %s keys\n", strconv.Itoa(x))
// 			doneCh <- x
// 			return
// 		default:
// 			k := key + "-" + strconv.Itoa(x)
// 			// write key
// 			// fmt.Fprintf(os.Stderr, "Write %s\n", createPath+k)
// 			err := writeKey(port, createPath+k)
// 			if err != nil {
// 				//error
// 			}
// 			// delete key
// 			// fmt.Fprintf(os.Stderr, "Delete %s\n", deletePath+k)
// 			err = writeKey(port, deletePath+k)
// 			if err != nil {
// 				//error
// 			}
// 			x++
// 			time.Sleep(100 * time.Millisecond)
// 			// leave network
// 			// fmt.Fprintf(os.Stderr, "%s Leave network\n", key)
// 			err = leaveNetwork("localhost", port, "test")
// 			if err != nil {
// 				//error
// 			}
// 			time.Sleep(100 * time.Millisecond)
// 			// join network
// 			// fmt.Fprintf(os.Stderr, "%s Join network\n", key)
// 			err = joinNetwork("localhost", port, "test")
// 			if err != nil {
// 				//error
// 			}
// 		}
// 		// time.Sleep(200 * time.Millisecond)
// 	}
// }
//
// func doWriteDeleteLeaveJoinSingle(ctx context.Context, port, key string, doneCh chan int) {
// 	x := 0
// 	createPath := "/createentry?nid=test&tname=table_name&value=v&key="
// 	// deletePath := "/deleteentry?nid=test&tname=table_name&key="
//
// 	fmt.Fprintf(os.Stderr, "%s Started\n", key)
//
// 	numEntries := 1000
// 	// Create 1000 entries
// 	for i := 0; i < numEntries; i++ {
// 		k := key + "-" + strconv.Itoa(x)
// 		// write key
// 		// fmt.Fprintf(os.Stderr, "Write %s\n", createPath+k)
// 		err := writeKey(port, createPath+k)
// 		if err != nil {
// 			//error
// 		}
// 		x++
// 	}
// 	// Delete all of them
// 	x = 0
// 	// for i := 0; i < 100; i++ {
// 	// 	k := key + "-" + strconv.Itoa(x)
// 	// 	// write key
// 	// 	// fmt.Fprintf(os.Stderr, "Write %s\n", createPath+k)
// 	// 	err := writeKey(port, deletePath+k)
// 	// 	if err != nil {
// 	// 		//error
// 	// 	}
// 	// 	x++
// 	// }
// 	// Leave the network
// 	err := leaveNetwork("localhost", port, "test")
// 	if err != nil {
// 		//error
// 	}
// 	time.Sleep(200 * time.Millisecond)
// 	// Join the network
// 	err = joinNetwork("localhost", port, "test")
// 	if err != nil {
// 		//error
// 	}
// 	fmt.Fprintf(os.Stderr, "Exiting after having done the procedure\n")
// 	doneCh <- x
// 	return
// }
//
// func writeAndDelete(writerList []string, keyBase string) {
// 	workers := len(writerList)
// 	doneCh := make(chan int)
// 	ctx, cancel := context.WithCancel(context.Background())
//
// 	// start the write in parallel
// 	for _, w := range writerList {
// 		key := keyBase + w
// 		fmt.Fprintf(os.Stderr, "Spawn worker: %s\n", w)
// 		go doWriteDelete(ctx, w, key, doneCh)
// 	}
// 	time.Sleep(10 * time.Second)
// 	cancel()
// 	for workers > 0 {
// 		fmt.Fprintf(os.Stderr, "Remains: %d workers\n", workers)
// 		<-doneCh
// 		workers--
// 	}
//
// 	// Stop when stable
// 	stableResult := 10
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
// 					stableResult = 10
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
// }

//
// func writeAndDeleteLeaveJoin(writerList []string, keyBase string) {
// 	workers := len(writerList)
// 	doneCh := make(chan int)
// 	ctx, cancel := context.WithCancel(context.Background())
//
// 	// start the write in parallel
// 	for _, w := range writerList {
// 		key := keyBase + w
// 		fmt.Fprintf(os.Stderr, "Spawn worker: %s\n", w)
// 		go doWriteDeleteLeaveJoin(ctx, w, key, doneCh)
// 	}
// 	time.Sleep(5 * time.Second)
// 	cancel()
// 	for workers > 0 {
// 		fmt.Fprintf(os.Stderr, "Remains: %d workers\n", workers)
// 		<-doneCh
// 		workers--
// 	}
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
		logrus.Infof("Spawn worker: %d", i)
		go writeDeleteUniqueKeys(ctx, ips[i], servicePort, key, networkName, tableName, doneCh)
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
	default:
		log.Fatalf("Command %s not recognized", command)
	}
}
