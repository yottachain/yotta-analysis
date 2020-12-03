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
	spotCheckInterval     int
	rwlock                *sync.RWMutex
	rwlock2               *sync.RWMutex
}

//NewNodeManager create new node manager
func NewNodeManager(ctx context.Context, cli *mongo.Client, taskManager *TaskManager, mqconf *AuraMQConfig, poolLength, queueLength int, minerVersionThreshold int32, avaliableNodeTimeGap int64, spotCheckInterval int64, excludeAddrPrefix string) (*NodeManager, error) {
	entry := log.WithFields(log.Fields{Function: "NewNodeManager"})
	nodeMgr := new(NodeManager)
	nodeMgr.analysisdbClient = cli
	nodeMgr.taskManager = taskManager
	nodeMgr.SelectableNodes = new(Int32Set)
	nodeMgr.Nodes = make(map[int32]*Node)
	nodeMgr.Tasks = make(map[int32]*SpotCheckItem)
	nodeMgr.rwlock = new(sync.RWMutex)
	nodeMgr.rwlock2 = new(sync.RWMutex)
	nodeMgr.minerVersionThreshold = int(minerVersionThreshold)
	nodeMgr.avaliableNodeTimeGap = int(avaliableNodeTimeGap)
	nodeMgr.spotCheckInterval = int(spotCheckInterval)
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
	//wg.Add(len(nodeMgr.Nodes))
	wg.Add(1000)
	idx := 0
	for _, node := range nodeMgr.Nodes {
		n := node
		idx++
		go func() {
			defer wg.Done()
			nodeMgr.PrepareTask(ctx, n)
		}()
		if idx%1000 == 0 {
			wg.Wait()
			wg = sync.WaitGroup{}
			wg.Add(1000)
			idx = 0
		}
	}
	for j := 0; j < 1000-idx; j++ {
		wg.Done()
	}
	if idx != 0 {
		wg.Wait()
	}
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
					err = nodeMgr.syncNode(ctx, node, excludeAddrPrefix)
					if err != nil {
						entry.WithError(err).Errorf("sync node %d", node.ID)
					} else {
						nodeMgr.UpdateNode(ctx, node)
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
	err = nodeMgr.calculateCD(ctx)
	if err != nil {
		entry.WithError(err).Error("calculate c and d failed")
		return nil, err
	}
	go func() {
		for {
			err := nodeMgr.calculateCD(ctx)
			if err != nil {
				entry.WithError(err).Warn("calculate c and d failed")
			}
			time.Sleep(time.Minute * 3)
		}
	}()
	return nodeMgr, nil
}

//GetSpotCheckTask get spotcgeck task
func (nodeMgr *NodeManager) GetSpotCheckTask(ctx context.Context) (*Node, *SpotCheckItem, error) {
	// entry := log.WithFields(log.Fields{Function: "GetSpotCheckTask"})
	// for i := 0; i < 10; i++ {
	id := nodeMgr.SelectableNodes.RandomDelete()
	entry := log.WithFields(log.Fields{Function: "GetSpotCheckTask", MinerID: id})
	if id == -1 {
		entry.Debug("can not find miner for spotchecking")
		return nil, nil, errors.New("no miner for spotchecking")
	}
	node := nodeMgr.Nodes[id]
	if node == nil {
		entry.Error("cannot find miner")
		return nil, nil, errors.New("cannot find miner")
	}
	if node.Status > 1 || node.Version < int32(nodeMgr.minerVersionThreshold) {
		nodeMgr.rwlock.Lock()
		delete(nodeMgr.Nodes, id)
		nodeMgr.rwlock.Unlock()
		nodeMgr.rwlock2.Lock()
		delete(nodeMgr.Tasks, id)
		nodeMgr.rwlock2.Unlock()
		entry.Info("remove miner")
		return nil, nil, errors.New("remove miner")
	}
	if node.Skip {
		go func() {
			nodeMgr.PrepareTask(ctx, node)
			nodeMgr.SelectableNodes.Add(node.ID)
		}()
		entry.Debug("retry get spotcheck item")
		return nil, nil, errors.New("retry get spotcheck item")
	}

	collectionS := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	lastSpotCheck := new(SpotCheckRecord)
	optionf := new(options.FindOptions)
	optionf.Sort = bson.M{"timestamp": -1}
	limit := int64(1)
	optionf.Limit = &limit
	cur, err := collectionS.Find(ctx, bson.M{"nid": node.ID}, optionf)
	if err != nil {
		entry.WithError(err).Errorf("fetching lastest spotcheck task of miner %d", node.ID)
		nodeMgr.SelectableNodes.Add(id)
		return nil, nil, err
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		err := cur.Decode(lastSpotCheck)
		if err != nil {
			entry.WithError(err).Errorf("decoding lastest spotcheck task of miner %d", node.ID)
			nodeMgr.SelectableNodes.Add(id)
			return nil, nil, err
		}
		entry.Debugf("found lastest spotcheck task of miner %d: %s", node.ID, lastSpotCheck.TaskID)
		if (time.Now().Unix()-lastSpotCheck.Timestamp < int64(nodeMgr.spotCheckInterval*60/2)) || lastSpotCheck.Status == 1 {
			entry.Warnf("conflict with lastest spotcheck task of miner %d: %s", node.ID, lastSpotCheck.TaskID)
			nodeMgr.SelectableNodes.Add(id)
			return nil, nil, fmt.Errorf("conflict with lastest spotcheck task of miner %d: %s", node.ID, lastSpotCheck.TaskID)
		}
	}

	item := nodeMgr.Tasks[node.ID]
	go func() {
		nodeMgr.PrepareTask(ctx, node)
		nodeMgr.SelectableNodes.Add(node.ID)
	}()
	// err = nodeMgr.taskManager.InsertSpotCheckItem(ctx, item)
	// if err != nil {
	// 	return nil, nil, err
	// }
	return node, item, nil
	// }
	// entry.Debug("can not find miner for spotchecking")
	// return nil, nil, errors.New("no miner for spotchecking")
}

//UpdateNode update node in node manager
func (nodeMgr *NodeManager) UpdateNode(ctx context.Context, node *Node) {
	entry := log.WithFields(log.Fields{Function: "UpdateNode", MinerID: node.ID})
	if node == nil || node.ID <= 0 {
		entry.Warn("invalid node")
		return
	}
	nodeMgr.rwlock.Lock()
	defer nodeMgr.rwlock.Unlock()
	oldNode := nodeMgr.Nodes[node.ID]
	if oldNode != nil {
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
		entry.Debugf("update miner info")
	} else {
		if node.Status == 1 && node.Version >= int32(nodeMgr.minerVersionThreshold) && node.UsedSpace > 0 {
			nodeMgr.Nodes[node.ID] = node
			go func() {
				nodeMgr.PrepareTask(ctx, node)
				nodeMgr.SelectableNodes.Add(node.ID)
			}()
			entry.Debugf("add new miner")
		}
	}
}

//PrepareTask prepare spotcheck task
func (nodeMgr *NodeManager) PrepareTask(ctx context.Context, node *Node) error {
	entry := log.WithFields(log.Fields{Function: "PrepareTask", MinerID: node.ID})
	st := time.Now().UnixNano()
	defer func() {
		entry.Debugf("prepare task cost %dms", (time.Now().UnixNano()-st)/1000000)
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
		entry.Debugf("find first shard %d", fs)
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

	nodeMgr.rwlock2.Lock()
	nodeMgr.Tasks[node.ID] = item
	nodeMgr.rwlock2.Unlock()
	if node.Skip {
		node.Skip = false
		err := nodeMgr.UpdateSkip(ctx, node.ID, false)
		if err != nil {
			entry.WithError(err).Errorf("update skip status to false for miner %d", node.ID)
		}
	}
	entry.Debugf("generated spotcheck item %s(%d)", item.CheckShard, len(item.ExtraShards))
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

func (nodeMgr *NodeManager) calculateCD(ctx context.Context) error {
	entry := log.WithFields(log.Fields{Function: "calculateCD"})
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	c, err := collection.CountDocuments(ctx, bson.M{"skip": false, "status": 1, "version": bson.M{"$gte": nodeMgr.minerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheckable miners")
		return errors.New("error when get count of spotcheckable miners")
	}
	atomic.StoreInt64(&nodeMgr.c, c)
	entry.Debugf("count of spotcheckable miners is %d", c)
	d, err := collection.CountDocuments(ctx, bson.M{"status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*int64(nodeMgr.avaliableNodeTimeGap)}, "version": bson.M{"$gte": nodeMgr.minerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheck-executing miners")
		return errors.New("error when get count of spotcheck-executing miners")
	}
	atomic.StoreInt64(&nodeMgr.d, d)
	entry.Debugf("count of spotcheck-executing miners is %d", d)
	return nil
}

func (nodeMgr *NodeManager) syncNode(ctx context.Context, node *Node, excludeAddrPrefix string) error {
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
	// _, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc, "skip": false})
	// if err != nil {
	// 	errstr := err.Error()
	// 	if !strings.ContainsAny(errstr, "duplicate key error") {
	// 		entry.WithError(err).Warnf("inserting node %d to Node table", node.ID)
	// 		return err
	// 	}
	cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
	for k, v := range node.Uspaces {
		cond[fmt.Sprintf("uspaces.%s", k)] = v
	}
	opts := new(options.UpdateOptions)
	//opts = opts.SetReturnDocument(options.Before)
	upsert := true
	opts.Upsert = &upsert
	_, err := collection.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$set": cond, "$setOnInsert": bson.M{"skip": false}}, opts)
	// oldNode := new(Node)
	// err := result.Decode(oldNode)
	if err != nil {
		entry.WithError(err).Warnf("updating record of node %d", node.ID)
		return err
	}
	//if oldNode.Status != node.Status {
	//collectionSN := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	_, err = collectionSN.UpdateOne(ctx, bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
	if err != nil {
		entry.WithError(err).Warnf("updating status of spotcheck node %d", node.ID)
		return err
	}
	//}
	//}
	// _, err = collectionSN.InsertOne(context.Background(), node)
	// if err != nil {
	// 	entry.WithError(err).Warnf("inserting node %d to SpotChecknode table", node.ID)
	// 	return err
	// }
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
