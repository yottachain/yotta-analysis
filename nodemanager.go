package ytanalysis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	pb "github.com/yottachain/yotta-analysis/pbanalysis"
	ytsync "github.com/yottachain/yotta-analysis/sync"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//NodeManager node manager struct
type NodeManager struct {
	analysisdbClient      *mongo.Client
	taskManager           *TaskManager
	SelectableNodes       *Int32Set
	Nodes                 map[int32]*Node
	Tasks                 map[int32]*SpotCheckItem
	MqClis                map[int]*ytsync.Service
	c                     int64
	d                     int64
	minerVersionThreshold int
	avaliableNodeTimeGap  int
	rwlock                *sync.RWMutex
}

//NewNodeManager create new node manager
func NewNodeManager(ctx context.Context, cli *mongo.Client, taskManager *TaskManager, mqconf *AuraMQConfig, poolLength, queueLength int, minerVersionThreshold int32, avaliableNodeTimeGap int64, excludeAddrPrefix string) (*NodeManager, error) {
	entry := log.WithFields(log.Fields{Function: "NewNodeManager"})
	nodeMgr := new(NodeManager)
	nodeMgr.analysisdbClient = cli
	nodeMgr.taskManager = taskManager
	nodeMgr.SelectableNodes = new(Int32Set)
	nodeMgr.Nodes = make(map[int32]*Node)
	nodeMgr.Tasks = make(map[int32]*SpotCheckItem)
	nodeMgr.rwlock = new(sync.RWMutex)
	nodeMgr.minerVersionThreshold = int(minerVersionThreshold)
	nodeMgr.avaliableNodeTimeGap = int(avaliableNodeTimeGap)
	collection := cli.Database(AnalysisDB).Collection(NodeTab)
	cur, err := collection.Find(ctx, bson.M{"status": 1})
	if err != nil {
		entry.WithError(err).Error("find all miners")
		return nil, err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		node := new(Node)
		err := cur.Decode(node)
		if err != nil {
			continue
		}
		if node.UsedSpace > 0 && int(node.Version) >= nodeMgr.minerVersionThreshold {
			nodeMgr.Nodes[node.ID] = node
			nodeMgr.SelectableNodes.Add(node.ID)
		}
	}
	entry.Info("nodes cache filled")
	var wg sync.WaitGroup
	wg.Add(len(nodeMgr.Nodes))
	for _, node := range nodeMgr.Nodes {
		n := node
		go func() {
			defer wg.Done()
			nodeMgr.PrepareTask(ctx, n)
		}()
	}
	wg.Wait()
	entry.Infof("cached %d miners", len(nodeMgr.Nodes))
	pool := grpool.NewPool(poolLength, queueLength)
	callback := func(msg *msg.Message) {
		if msg.GetType() == auramq.BROADCAST {
			if msg.GetDestination() == mqconf.MinerSyncTopic {
				pool.JobQueue <- func() {
					nodemsg := new(pb.NodeMsg)
					err := proto.Unmarshal(msg.Content, nodemsg)
					if err != nil {
						entry.WithError(err).Error("decoding nodeMsg failed")
						return
					}
					node := new(Node)
					node.Fillby(nodemsg)
					err = nodeMgr.syncNode(node, excludeAddrPrefix)
					if err != nil {
						entry.WithError(err).Errorf("sync node %d", node.ID)
					} else {
						nodeMgr.UpdateNode(ctx, node)
						// if node.Status == 1 && node.UsedSpace > 0 && int(node.Version) >= nodeMgr.minerVersionThreshold {
						// 	nodeMgr.UpdateNode(ctx, node)
						// } else {
						// 	nodeMgr.MarkDeleteNode(node)
						// }
					}
				}
			}
		}
	}
	m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	if err != nil {
		entry.WithError(err).Error("creating mq clients map failed")
		return nil, err
	}
	nodeMgr.MqClis = m
	err = nodeMgr.calculateCD()
	if err != nil {
		entry.WithError(err).Error("calculate c and d failed")
		return nil, err
	}
	go func() {
		for {
			err := nodeMgr.calculateCD()
			if err != nil {
				entry.WithError(err).Warn("calculate c and d failed")
			}
			time.Sleep(time.Minute * 10)
		}
	}()
	return nodeMgr, nil
}

//GetSpotCheckTask get spotcgeck task
func (nodeMgr *NodeManager) GetSpotCheckTask(ctx context.Context) (*Node, SpotCheckItem, error) {
	for i := 0; i < 10; i++ {
		id := nodeMgr.SelectableNodes.RandomDelete()
		entry := log.WithFields(log.Fields{Function: "GetSpotCheckTask", MinerID: id})
		node := nodeMgr.Nodes[id]
		if node == nil {
			entry.Debugf("cannot find miner")
			continue
		}
		node.Lock.Lock()
		if node.Processing {

		}
	}
}

//UpdateNode update node in node manager
func (nodeMgr *NodeManager) UpdateNode(ctx context.Context, node *Node) {
	entry := log.WithFields(log.Fields{Function: "UpdateNode", MinerID: node.ID})
	if node == nil || node.ID <= 0 {
		entry.Warn("invalid node")
		return
	}
	nodeMgr.rwlock.Lock()
	oldNode := nodeMgr.Nodes[node.ID]
	if oldNode != nil {
		nodeMgr.rwlock.Unlock()
		oldNode.Lock.Lock()
		defer oldNode.Lock.Unlock()
		oldNode.NodeID = node.NodeID
		oldNode.PubKey = node.PubKey
		oldNode.Owner = node.Owner
		oldNode.ProfitAcc = node.ProfitAcc
		oldNode.PoolID = node.PoolID
		oldNode.PoolOwner = node.PoolOwner
		oldNode.Quota = node.Quota
		oldNode.Addrs = node.Addrs
		oldNode.CPU = node.CPU
		oldNode.Memory = node.Memory
		oldNode.Bandwidth = node.Bandwidth
		oldNode.MaxDataSpace = node.MaxDataSpace
		oldNode.AssignedSpace = node.AssignedSpace
		oldNode.ProductiveSpace = node.ProductiveSpace
		oldNode.UsedSpace = node.UsedSpace
		oldNode.Uspaces = node.Uspaces
		oldNode.Weight = node.Weight
		oldNode.Valid = node.Valid
		oldNode.Relay = node.Relay
		oldNode.Status = node.Status
		oldNode.Timestamp = node.Timestamp
		oldNode.Version = node.Version
		oldNode.Rebuilding = node.Rebuilding
		oldNode.RealSpace = node.RealSpace
		oldNode.Tx = node.Tx
		oldNode.Rx = node.Rx
		oldNode.ErrorCount = node.ErrorCount
	} else {
		nodeMgr.Nodes[node.ID] = node
		node.Processing = true
		go func() {
			nodeMgr.PrepareTask(ctx, node)
			nodeMgr.SelectableNodes.Add(node.ID)
		}()
		nodeMgr.rwlock.Unlock()
		// node.Lock.Lock()
		// defer node.Lock.Unlock()
		// if node.Processing {
		// 	return
		// }
		// node.Processing = true

	}
}

//PrepareTask prepare spotcheck task
func (nodeMgr *NodeManager) PrepareTask(ctx context.Context, node *Node) error {
	entry := log.WithFields(log.Fields{Function: "UpdateNode", MinerID: node.ID})
	defer func() {
		node.Processing = false
	}()
	if node.FirstShard == 0 {
		fs, err := nodeMgr.taskManager.FirstShard(ctx, node.ID)
		if err != nil {
			entry.WithError(err).Errorf("miner %d has no shards for spotchecking", node.ID)
			if !node.Skip {
				node.Skip = true
				err := nodeMgr.UpdateSkip(ctx, node.ID, true)
				if err != nil {
					entry.WithError(err).Errorf("update skip status to true for miner %d", node.ID)
				}
			}
			return err
		}
		node.FirstShard = fs
		err = nodeMgr.UpdateFirstShard(ctx, node.ID, fs)
		if err != nil {
			entry.WithError(err).Errorf("update skip first shard to %d for miner %d", fs, node.ID)
		}
	}
	item, err := nodeMgr.taskManager.GetSpotCheckItem(ctx, node)
	if err != nil {
		entry.Warnf("miner %d has no shards for spotchecking", node.ID)
		if !node.Skip {
			node.Skip = true
			err := nodeMgr.UpdateSkip(ctx, node.ID, true)
			if err != nil {
				entry.WithError(err).Errorf("update skip status to true for miner %d", node.ID)
			}
		}
		return err
	}
	nodeMgr.Tasks[node.ID] = item
	if node.Skip {
		node.Skip = false
		err := nodeMgr.UpdateSkip(ctx, node.ID, false)
		if err != nil {
			entry.WithError(err).Errorf("update skip status to false for miner %d", node.ID)
		}
	}
	entry.Debugf("miner %d generated spotcheck item", node.ID)
	return nil
}

//UpdateSkip update skip parameter of miner
func (nodeMgr *NodeManager) UpdateSkip(ctx context.Context, minerID int32, skip bool) error {
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	_, err := collection.UpdateOne(ctx, bson.M{"_id": minerID}, bson.M{"$set": bson.M{"skip": skip}})
	return err
}

//UpdateFirstShard update firstShard parameter of miner
func (nodeMgr *NodeManager) UpdateFirstShard(ctx context.Context, minerID int32, firstShard int64) error {
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	_, err := collection.UpdateOne(ctx, bson.M{"_id": minerID}, bson.M{"$set": bson.M{"firstShard": firstShard}})
	return err
}

//DeleteNode delete node in node manager
// func (nodeMgr *NodeManager) MarkDeleteNode(node *Node) {
// 	nodeMgr.rwlock.Lock()
// 	defer nodeMgr.rwlock.Unlock()
// 	delete(nodeMgr.Nodes, id)

// 	entry := log.WithFields(log.Fields{Function: "MarkDeleteNode", MinerID: node.ID})
// 	if node.ID <= 0 {
// 		entry.Warn("invalid miner ID")
// 		return
// 	}
// 	nodeMgr.rwlock.Lock()
// 	defer nodeMgr.rwlock.Unlock()
// 	oldNode := nodeMgr.Nodes[node.ID]
// 	if oldNode != nil {
// 		oldNode.Lock.Lock()
// 		oldNode.Status=node.Status
// 		oldNode.
// 	}
// }

//GetNode get node by node ID
func (nodeMgr *NodeManager) GetNode(id int32) *Node {
	nodeMgr.rwlock.RLock()
	defer nodeMgr.rwlock.RUnlock()
	return nodeMgr.Nodes[id]
}

func (nodeMgr *NodeManager) calculateCD() error {
	entry := log.WithFields(log.Fields{Function: "calculateCD"})
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"skip": false, "status": 1, "version": bson.M{"$gte": nodeMgr.minerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheckable miners")
		return errors.New("error when get count of spotcheckable miners")
	}
	atomic.StoreInt64(&nodeMgr.c, c)
	entry.Debugf("count of spotcheckable miners is %d", c)
	d, err := collection.CountDocuments(context.Background(), bson.M{"status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*int64(nodeMgr.avaliableNodeTimeGap)}, "version": bson.M{"$gte": nodeMgr.minerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheck-executing miners")
		return errors.New("error when get count of spotcheck-executing miners")
	}
	atomic.StoreInt64(&nodeMgr.d, d)
	entry.Debugf("count of spotcheck-executing miners is %d", d)
	return nil
}

func (nodeMgr *NodeManager) syncNode(node *Node, excludeAddrPrefix string) error {
	entry := log.WithFields(log.Fields{Function: "syncNode"})
	if node.ID == 0 {
		return errors.New("node ID cannot be 0")
	}
	node.Addrs = checkPublicAddrs(node.Addrs, excludeAddrPrefix)
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	collectionSN := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	otherDoc := bson.A{}
	if node.Ext != "" && node.Ext[0] == '[' && node.Ext[len(node.Ext)-1] == ']' {
		var bdoc interface{}
		err := bson.UnmarshalExtJSON([]byte(node.Ext), true, &bdoc)
		if err != nil {
			entry.WithError(err).Warn("parse ext document")
		} else {
			otherDoc, _ = bdoc.(bson.A)
		}
	}
	if node.Uspaces == nil {
		node.Uspaces = make(map[string]int64)
	}
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc, "skip": false})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			entry.WithError(err).Warnf("inserting node %d to Node table", node.ID)
			return err
		}
		cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
		for k, v := range node.Uspaces {
			cond[fmt.Sprintf("uspaces.%s", k)] = v
		}
		opts := new(options.FindOneAndUpdateOptions)
		opts = opts.SetReturnDocument(options.Before)
		result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
		oldNode := new(Node)
		err := result.Decode(oldNode)
		if err != nil {
			entry.WithError(err).Warnf("updating record of node %d", node.ID)
			return err
		}
		if oldNode.Status != node.Status {
			collectionSN := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
			_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": node.Status}})
			if err != nil {
				entry.WithError(err).Warnf("updating status of spotcheck node %d", node.ID)
				return err
			}
		}
	}
	_, err = collectionSN.InsertOne(context.Background(), node)
	if err != nil {
		entry.WithError(err).Warnf("inserting node %d to SpotChecknode table", node.ID)
		return err
	}
	return nil
}

func checkPublicAddrs(addrs []string, excludeAddrPrefix string) []string {
	filteredAddrs := []string{}
	for _, addr := range addrs {
		if strings.HasPrefix(addr, "/ip4/127.") ||
			strings.HasPrefix(addr, "/ip4/192.168.") ||
			strings.HasPrefix(addr, "/ip4/169.254.") ||
			strings.HasPrefix(addr, "/ip4/10.") ||
			strings.HasPrefix(addr, "/ip4/172.16.") ||
			strings.HasPrefix(addr, "/ip4/172.17.") ||
			strings.HasPrefix(addr, "/ip4/172.18.") ||
			strings.HasPrefix(addr, "/ip4/172.19.") ||
			strings.HasPrefix(addr, "/ip4/172.20.") ||
			strings.HasPrefix(addr, "/ip4/172.21.") ||
			strings.HasPrefix(addr, "/ip4/172.22.") ||
			strings.HasPrefix(addr, "/ip4/172.23.") ||
			strings.HasPrefix(addr, "/ip4/172.24.") ||
			strings.HasPrefix(addr, "/ip4/172.25.") ||
			strings.HasPrefix(addr, "/ip4/172.26.") ||
			strings.HasPrefix(addr, "/ip4/172.27.") ||
			strings.HasPrefix(addr, "/ip4/172.28.") ||
			strings.HasPrefix(addr, "/ip4/172.29.") ||
			strings.HasPrefix(addr, "/ip4/172.30.") ||
			strings.HasPrefix(addr, "/ip4/172.31.") ||
			strings.HasPrefix(addr, "/ip6/") ||
			strings.HasPrefix(addr, "/p2p-circuit/") {
			if excludeAddrPrefix != "" && strings.HasPrefix(addr, excludeAddrPrefix) {
				filteredAddrs = append(filteredAddrs, addr)
			}
			continue
		} else {
			filteredAddrs = append(filteredAddrs, addr)
		}
	}
	return dedup(filteredAddrs)
}

func dedup(urls []string) []string {
	if urls == nil || len(urls) == 0 {
		return nil
	}
	sort.Strings(urls)
	j := 0
	for i := 1; i < len(urls); i++ {
		if urls[j] == urls[i] {
			continue
		}
		j++
		urls[j] = urls[i]
	}
	return urls[:j+1]
}
