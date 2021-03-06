package ytanalysis

import (
	pb "github.com/yottachain/yotta-analysis/pb"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Node instance
type Node struct {
	//data node index
	ID int32 `bson:"_id"`
	//data node ID, generated from PubKey
	NodeID string `bson:"nodeid"`
	//public key of data node
	PubKey string `bson:"pubkey"`
	//owner account of this miner
	Owner string `bson:"owner"`
	//profit account of this miner
	ProfitAcc string `bson:"profitAcc"`
	//ID of associated miner pool
	PoolID string `bson:"poolID"`
	//Owner of associated miner pool
	PoolOwner string `bson:"poolOwner"`
	//quota allocated by associated miner pool
	Quota int64 `bson:"quota"`
	//listening addresses of data node
	Addrs []string `bson:"addrs"`
	//CPU usage of data node
	CPU int32 `bson:"cpu"`
	//memory usage of data node
	Memory int32 `bson:"memory"`
	//bandwidth usage of data node
	Bandwidth int32 `bson:"bandwidth"`
	//max space of data node
	MaxDataSpace int64 `bson:"maxDataSpace"`
	//space assigned to YTFS
	AssignedSpace int64 `bson:"assignedSpace"`
	//pre-allocated space of data node
	ProductiveSpace int64 `bson:"productiveSpace"`
	//used space of data node
	UsedSpace int64 `bson:"usedSpace"`
	//weight for allocate data node
	Weight float64 `bson:"weight"`
	//Is node valid
	Valid int32 `bson:"valid"`
	//Is relay node
	Relay int32 `bson:"relay"`
	//status code: 0 - registered 1 - active
	Status int32 `bson:"status"`
	//timestamp of status updating operation
	Timestamp int64 `bson:"timestamp"`
	//version number of miner
	Version int32 `bson:"version"`
	//Rebuilding if node is under rebuilding
	Rebuilding int32 `bson:"rebuilding"`
	//RealSpace real space of miner
	RealSpace int64 `bson:"realSpace"`
	//Tx
	Tx int64 `bson:"tx"`
	//Rx
	Rx int64 `bson:"rx"`
	//ErrorCount
	ErrorCount int32 `bson:"errorCount"`
}

//NewNode create a node struct
func NewNode(id int32, nodeid string, pubkey string, owner string, profitAcc string, poolID string, poolOwner string, quota int64, addrs []string, cpu int32, memory int32, bandwidth int32, maxDataSpace int64, assignedSpace int64, productiveSpace int64, usedSpace int64, weight float64, valid int32, relay int32, status int32, timestamp int64, version int32, rebuilding int32, realSpace int64, tx int64, rx int64) *Node {
	return &Node{ID: id, NodeID: nodeid, PubKey: pubkey, Owner: owner, ProfitAcc: profitAcc, PoolID: poolID, PoolOwner: poolOwner, Quota: quota, Addrs: addrs, CPU: cpu, Memory: memory, Bandwidth: bandwidth, MaxDataSpace: maxDataSpace, AssignedSpace: assignedSpace, ProductiveSpace: productiveSpace, UsedSpace: usedSpace, Weight: weight, Valid: valid, Relay: relay, Status: status, Timestamp: timestamp, Version: version, Rebuilding: rebuilding, RealSpace: realSpace, Tx: tx, Rx: rx}
}

//SuperNode instance
type SuperNode struct {
	//super node index
	ID int32 `bson:"_id"`
	//super node ID, generated from PubKey
	NodeID string `bson:"nodeid"`
	//public key of super node
	PubKey string `bson:"pubkey"`
	//private key of super node
	PrivKey string `bson:"privkey"`
	//listening addresses of super node
	Addrs []string `bson:"addrs"`
}

//NodeStat statistics of data node
type NodeStat struct {
	ActiveMiners    int64 `bson:"activeMiners"`
	TotalMiners     int64 `bson:"totalMiners"`
	MaxTotal        int64 `bson:"maxTotal"`
	AssignedTotal   int64 `bson:"assignedTotal"`
	ProductiveTotal int64 `bson:"productiveTotal"`
	UsedTotal       int64 `bson:"usedTotal"`
}

//ShardCount shards count of one data node
type ShardCount struct {
	ID  int32 `bson:"_id"`
	Cnt int64 `bson:"cnt"`
}

//SpotCheckList list of spot check
type SpotCheckList struct {
	TaskID    primitive.ObjectID `bson:"_id"`
	TaskList  []*SpotCheckTask   `bson:"taskList"`
	Timestamp int64              `bson:"timestamp"`
}

//SpotCheckTask one spot check task
type SpotCheckTask struct {
	ID     int32  `bson:"id"`
	NodeID string `bson:"nodeid"`
	Addr   string `bson:"addr"`
	VNI    string `bson:"vni"`
}

//SpotCheckRecord spot check task in database
type SpotCheckRecord struct {
	TaskID    string `bson:"_id"`
	NID       int32  `bson:"nid"`
	VNI       string `bson:"vni"`
	Status    int32  `bson:"status"` // 0 - send to client, 1 - receive invalid node, 2 - rechecking
	Timestamp int64  `bson:"timestamp"`
	//ErrCount  int64  `bson:"errCount"`
}

//DNI dni struct
type DNI struct {
	ID      primitive.ObjectID `bson:"_id"`
	Shard   primitive.Binary   `bson:"shard"`
	MinerID int32              `bson:"minerID"`
	Delete  int32              `bson:"delete"`
}

//VNI vni struct
type VNI struct {
	ID  int32  `bson:"_id"`
	VNI []byte `bson:"vni"`
}

//PoolWeight infomation of pool
type PoolWeight struct {
	ID                string `bson:"_id"`
	PoolReferralSpace int64  `bson:"poolReferralSpace"`
	PoolTotalSpace    int64  `bson:"poolTotalSpace"`
	ReferralSpace     int64  `bson:"referralSpace"`
	TotalSpace        int64  `bson:"totalSpace"`
	PoolTotalCount    int64  `bson:"poolTotalCount"`
	PoolErrorCount    int64  `bson:"poolErrorCount"`
	Timestamp         int64  `bson:"timestamp"`
	ManualWeight      int64  `bson:"manualWeight"`
}

//relative DB and collection name
var (
	YottaDB          = "yotta"
	NodeTab          = "Node"
	SuperNodeTab     = "SuperNode"
	DNITab           = "Shards"
	SequenceTab      = "Sequence"
	PoolWeightTab    = "PoolWeight"
	SpaceSumTab      = "SpaceSum"
	AnalysisDB       = "analysis"
	SpotCheckTab     = "SpotCheck"
	SpotCheckNodeTab = "SpotCheckNode"
)

//index type of node and supernode collection
var (
	NodeIdxType      = 100
	SuperNodeIdxType = 101
)

//IntervalTime interval time of data node reporting status
var IntervalTime int64 = 60

// Convert convert Node strcut to NodeMsg
func (node *Node) Convert() *pb.NodeMsg {
	return &pb.NodeMsg{
		ID:              node.ID,
		NodeID:          node.NodeID,
		PubKey:          node.PubKey,
		Owner:           node.Owner,
		ProfitAcc:       node.ProfitAcc,
		PoolID:          node.PoolID,
		PoolOwner:       node.PoolOwner,
		Quota:           node.Quota,
		Addrs:           node.Addrs,
		CPU:             node.CPU,
		Memory:          node.Memory,
		Bandwidth:       node.Bandwidth,
		MaxDataSpace:    node.MaxDataSpace,
		AssignedSpace:   node.AssignedSpace,
		ProductiveSpace: node.ProductiveSpace,
		UsedSpace:       node.UsedSpace,
		Weight:          node.Weight,
		Valid:           node.Valid,
		Relay:           node.Relay,
		Status:          node.Status,
		Timestamp:       node.Timestamp,
		Version:         node.Version,
		Rebuilding:      node.Rebuilding,
		RealSpace:       node.RealSpace,
		Tx:              node.Tx,
		Rx:              node.Rx,
	}
}

// Fillby convert NodeMsg to Node struct
func (node *Node) Fillby(msg *pb.NodeMsg) {
	node.ID = msg.ID
	node.NodeID = msg.NodeID
	node.PubKey = msg.PubKey
	node.Owner = msg.Owner
	node.ProfitAcc = msg.ProfitAcc
	node.PoolID = msg.PoolID
	node.PoolOwner = msg.PoolOwner
	node.Quota = msg.Quota
	node.Addrs = msg.Addrs
	node.CPU = msg.CPU
	node.Memory = msg.Memory
	node.Bandwidth = msg.Bandwidth
	node.MaxDataSpace = msg.MaxDataSpace
	node.AssignedSpace = msg.AssignedSpace
	node.ProductiveSpace = msg.ProductiveSpace
	node.UsedSpace = msg.UsedSpace
	node.Weight = msg.Weight
	node.Valid = msg.Valid
	node.Relay = msg.Relay
	node.Status = msg.Status
	node.Timestamp = msg.Timestamp
	node.Version = msg.Version
	node.Rebuilding = msg.Rebuilding
	node.RealSpace = msg.RealSpace
	node.Tx = msg.Tx
	node.Rx = msg.Rx
}

// ConvertNodesToNodesMsg convert list of Node to list of NodeMsg
func ConvertNodesToNodesMsg(nodes []*Node) []*pb.NodeMsg {
	nodeMsgs := make([]*pb.NodeMsg, len(nodes))
	for i, n := range nodes {
		nodeMsgs[i] = n.Convert()
	}
	return nodeMsgs
}

// Convert convert SuperNode strcut to SuperNodeMsg
func (superNode *SuperNode) Convert() *pb.SuperNodeMsg {
	return &pb.SuperNodeMsg{
		ID:      superNode.ID,
		NodeID:  superNode.NodeID,
		PubKey:  superNode.PubKey,
		PrivKey: superNode.PrivKey,
		Addrs:   superNode.Addrs,
	}
}

// Fillby convert SuperNodeMsg to SuperNode struct
func (superNode *SuperNode) Fillby(msg *pb.SuperNodeMsg) {
	superNode.ID = msg.ID
	superNode.NodeID = msg.NodeID
	superNode.PubKey = msg.PubKey
	superNode.PrivKey = msg.PrivKey
	superNode.Addrs = msg.Addrs
}

// ConvertSuperNodesToSuperNodesMsg convert list of SuperNode to list of SuperNodeMsg
func ConvertSuperNodesToSuperNodesMsg(superNodes []*SuperNode) []*pb.SuperNodeMsg {
	superNodeMsgs := make([]*pb.SuperNodeMsg, len(superNodes))
	for i, s := range superNodes {
		superNodeMsgs[i] = s.Convert()
	}
	return superNodeMsgs
}

// Convert convert NodeStat strcut to NodeStatMsg
func (nodeStat *NodeStat) Convert() *pb.NodeStatMsg {
	return &pb.NodeStatMsg{
		ActiveMiners:    nodeStat.ActiveMiners,
		TotalMiners:     nodeStat.TotalMiners,
		MaxTotal:        nodeStat.MaxTotal,
		AssignedTotal:   nodeStat.AssignedTotal,
		ProductiveTotal: nodeStat.ProductiveTotal,
		UsedTotal:       nodeStat.UsedTotal,
	}
}

// Fillby convert NodeMsg to Node struct
func (nodeStat *NodeStat) Fillby(msg *pb.NodeStatMsg) {
	nodeStat.ActiveMiners = msg.ActiveMiners
	nodeStat.TotalMiners = msg.TotalMiners
	nodeStat.MaxTotal = msg.MaxTotal
	nodeStat.AssignedTotal = msg.AssignedTotal
	nodeStat.ProductiveTotal = msg.ProductiveTotal
	nodeStat.UsedTotal = msg.UsedTotal
}

// Convert convert ShardCount strcut to ShardCountMsg
func (shardCount *ShardCount) Convert() *pb.ShardCountMsg {
	return &pb.ShardCountMsg{
		ID:  shardCount.ID,
		Cnt: shardCount.Cnt,
	}
}

// Fillby convert ShardCountMsg to ShardCount
func (shardCount *ShardCount) Fillby(msg *pb.ShardCountMsg) {
	shardCount.ID = msg.ID
	shardCount.Cnt = msg.Cnt
}

// ConvertShardCountsToShardCountsMsg convert list of ShardCount to list of ShardCountMsg
func ConvertShardCountsToShardCountsMsg(shardCounts []*ShardCount) []*pb.ShardCountMsg {
	shardCountMsgs := make([]*pb.ShardCountMsg, len(shardCounts))
	for i, s := range shardCounts {
		shardCountMsgs[i] = s.Convert()
	}
	return shardCountMsgs
}

// Convert convert SpotCheckTask strcut to SpotCheckTaskMsg
func (spotCheckTask *SpotCheckTask) Convert() *pb.SpotCheckTaskMsg {
	return &pb.SpotCheckTaskMsg{
		ID:     spotCheckTask.ID,
		NodeID: spotCheckTask.NodeID,
		Addr:   spotCheckTask.Addr,
		VNI:    spotCheckTask.VNI,
	}
}

// Fillby convert SpotCheckTaskMsg to SpotCheckTask struct
func (spotCheckTask *SpotCheckTask) Fillby(msg *pb.SpotCheckTaskMsg) {
	spotCheckTask.ID = msg.ID
	spotCheckTask.NodeID = msg.NodeID
	spotCheckTask.Addr = msg.Addr
	spotCheckTask.VNI = msg.VNI
}

// ConvertSpotCheckTasksToSpotCheckTasksMsg convert list of ShardCount to list of ShardCountMsg
func ConvertSpotCheckTasksToSpotCheckTasksMsg(spotCheckTasks []*SpotCheckTask) []*pb.SpotCheckTaskMsg {
	spotCheckTaskMsgs := make([]*pb.SpotCheckTaskMsg, len(spotCheckTasks))
	for i, s := range spotCheckTasks {
		spotCheckTaskMsgs[i] = s.Convert()
	}
	return spotCheckTaskMsgs
}

// Convert convert SpotCheckTask strcut to SpotCheckTaskMsg
func (spotCheckList *SpotCheckList) Convert() *pb.SpotCheckListMsg {
	return &pb.SpotCheckListMsg{
		TaskID:    spotCheckList.TaskID.Hex(),
		TaskList:  ConvertSpotCheckTasksToSpotCheckTasksMsg(spotCheckList.TaskList),
		Timestamp: spotCheckList.Timestamp,
	}
}

// ConvertSpotCheckListsToSpotCheckListsMsg convert list of SpotCheckList to list of SpotCheckListMsg
func ConvertSpotCheckListsToSpotCheckListsMsg(spotCheckLists []*SpotCheckList) []*pb.SpotCheckListMsg {
	spotCheckListMsgs := make([]*pb.SpotCheckListMsg, len(spotCheckLists))
	for i, s := range spotCheckLists {
		spotCheckListMsgs[i] = s.Convert()
	}
	return spotCheckListMsgs
}
