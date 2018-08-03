package networkdb

import (
	"encoding/json"
	"net"

	"github.com/Sirupsen/logrus"
	metrics "github.com/docker/go-metrics"
	"github.com/hashicorp/memberlist"
)

var (
	ns = metrics.NewNamespace("libnetwork", "networkdb", nil)

	activeMetric metrics.Gauge
	failedMetric metrics.Gauge
	leftMetric   metrics.Gauge

	bulkSyncReplyFailures metrics.Counter
	bulkSyncReplySuccess metrics.Timer
)

func init() {
	nodes := ns.NewLabeledGauge("nodes", "The number of nodes in the gossip list", metrics.Total, "state")

	bulkSyncReplyFailures = ns.NewCounter("bulksync_reply_failures", "Number of timeouts waiting for a bulk sync reply")
	bulkSyncReplySuccess = ns.NewTimer("bulksync_reply_time", "Time spent waiting for a (successful) bulk sync reply")

	metrics.Register(ns)

	activeMetric = nodes.WithValues("active")
	failedMetric = nodes.WithValues("failed")
	leftMetric = nodes.WithValues("left")
}

type eventDelegate struct {
	nDB *NetworkDB
}

// Called with nDB lock held
func (nDB *NetworkDB) updateNodesMetric() {
	activeMetric.Set(float64(len(nDB.nodes)))
	failedMetric.Set(float64(len(nDB.failedNodes)))
	leftMetric.Set(float64(len(nDB.leftNodes)))
}

func (e *eventDelegate) broadcastNodeEvent(addr net.IP, op opType) {
	value, err := json.Marshal(&NodeAddr{addr})
	if err == nil {
		e.nDB.broadcaster.Write(makeEvent(op, NodeTable, "", "", value))
	} else {
		logrus.Errorf("Error marshalling node broadcast event %s", addr.String())
	}
}

func (e *eventDelegate) NotifyJoin(mn *memberlist.Node) {
	logrus.Infof("Node %s/%s, joined gossip cluster", mn.Name, mn.Addr)
	e.broadcastNodeEvent(mn.Addr, opCreate)
	e.nDB.Lock()
	defer e.nDB.Unlock()

	// In case the node is rejoining after a failure or leave,
	// just add the node back to active
	if moved, _ := e.nDB.changeNodeState(mn.Name, nodeActiveState); moved {
		return
	}

	// Every node has a unique ID
	// Check on the base of the IP address if the new node that joined is actually a new incarnation of a previous
	// failed or shutdown one
	e.nDB.purgeReincarnation(mn)
	e.nDB.nodes[mn.Name] = &node{Node: *mn}
	e.nDB.updateNodesMetric()
	logrus.Infof("Node %s/%s, added to nodes list", mn.Name, mn.Addr)
}

func (e *eventDelegate) NotifyLeave(mn *memberlist.Node) {
	logrus.Infof("Node %s/%s, left gossip cluster", mn.Name, mn.Addr)
	e.broadcastNodeEvent(mn.Addr, opDelete)

	e.nDB.Lock()
	defer e.nDB.Unlock()

	n, currState, _ := e.nDB.findNode(mn.Name)
	if n == nil {
		logrus.Errorf("Node %s/%s not found in the node lists", mn.Name, mn.Addr)
		return
	}
	// if the node was active means that did not send the leave cluster message, so it's probable that
	// failed. Else would be already in the left list so nothing else has to be done
	if currState == nodeActiveState {
		moved, err := e.nDB.changeNodeState(mn.Name, nodeFailedState)
		if err != nil {
			logrus.WithError(err).Errorf("impossible condition, node %s/%s not present in the list", mn.Name, mn.Addr)
			return
		}
		if moved {
			logrus.Infof("Node %s/%s, added to failed nodes list", mn.Name, mn.Addr)
		}
	}
}

func (e *eventDelegate) NotifyUpdate(n *memberlist.Node) {
}
