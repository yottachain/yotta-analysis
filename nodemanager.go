package ytanalysis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//NodeManager node manager struct
type NodeManager struct {
	analysisdbClient *mongo.Client
	taskManager      *TaskManager
	SelectableNodes  *Int32Set
	Nodes            map[int32]*Node
	Tasks            map[int32]*SpotCheckItem
	//MqClis                map[int]*ytsync.Service
	c                     int64
	d                     int64
	minerVersionThreshold int
	avaliableNodeTimeGap  int
	spotCheckInterval     int
	retryTimeDelay        int
	minerTrackBatchSize   int
	minerTrackInterval    int
	prepareTaskPoolSize   int
	calculateCDInterval   int
	rwlock                *sync.RWMutex
	rwlock2               *sync.RWMutex
}

//NewNodeManager create new node manager
func NewNodeManager(ctx context.Context, cli *mongo.Client, taskManager *TaskManager, minertrackerURL string, poolLength, queueLength int, minerVersionThreshold int32, avaliableNodeTimeGap int64, spotCheckInterval int64, excludeAddrPrefix string, retryTimeDelay, minerTrackBatchSize, minerTrackInterval, prepareTaskPoolSize, calculateCDInterval int) (*NodeManager, error) {
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
	nodeMgr.retryTimeDelay = retryTimeDelay
	nodeMgr.minerTrackBatchSize = minerTrackBatchSize
	nodeMgr.minerTrackInterval = minerTrackInterval
	nodeMgr.prepareTaskPoolSize = prepareTaskPoolSize
	nodeMgr.calculateCDInterval = calculateCDInterval

	err := nodeMgr.TrackMiners(ctx, minertrackerURL, excludeAddrPrefix)
	if err != nil {
		entry.WithError(err).Error("start tracking miners")
		return nil, err
	}
	entry.Info("start tracking miners")

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
	wg.Add(nodeMgr.prepareTaskPoolSize)
	idx := 0
	for _, node := range nodeMgr.Nodes {
		n := node
		idx++
		go func() {
			defer wg.Done()
			nodeMgr.PrepareTask(ctx, n)
		}()
		if idx%nodeMgr.prepareTaskPoolSize == 0 {
			wg.Wait()
			wg = sync.WaitGroup{}
			wg.Add(nodeMgr.prepareTaskPoolSize)
			idx = 0
		}
	}
	for j := 0; j < nodeMgr.prepareTaskPoolSize-idx; j++ {
		wg.Done()
	}
	if idx != 0 {
		wg.Wait()
	}
	entry.Infof("cached %d miners", len(nodeMgr.Nodes))

	// pool := grpool.NewPool(poolLength, queueLength)
	// callback := func(msg *msg.Message) {
	// 	if msg.GetType() == auramq.BROADCAST {
	// 		if msg.GetDestination() == mqconf.MinerSyncTopic {
	// 			pool.JobQueue <- func() {
	// 				nodemsg := new(pb.NodeMsg)
	// 				err := proto.Unmarshal(msg.Content, nodemsg)
	// 				if err != nil {
	// 					entry.WithError(err).Error("decoding nodeMsg failed")
	// 					return
	// 				}
	// 				node := new(Node)
	// 				node.Fillby(nodemsg)
	// 				err = nodeMgr.syncNode(ctx, node, excludeAddrPrefix)
	// 				if err != nil {
	// 					entry.WithError(err).Errorf("sync node %d", node.ID)
	// 				} else {
	// 					nodeMgr.UpdateNode(ctx, node)
	// 				}
	// 			}
	// 		}
	// 	}
	// }
	// m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	// if err != nil {
	// 	entry.WithError(err).Error("creating mq clients map failed")
	// 	return nil, err
	// }
	// nodeMgr.MqClis = m

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
			time.Sleep(time.Minute * time.Duration(nodeMgr.calculateCDInterval))
		}
	}()
	return nodeMgr, nil
}

//TrackMiners tracking status of miners from miner tracker service
func (nodeMgr *NodeManager) TrackMiners(ctx context.Context, minertrackerURL, excludeAddrPrefix string) error {
	entry := log.WithFields(log.Fields{Function: "TrackMiners"})
	httpCli := &http.Client{}
	collection := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	collectionProgress := nodeMgr.analysisdbClient.Database(AnalysisDB).Collection(TrackProgressTab)
	minersCnt, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		entry.WithError(err).Error("calculate count of miners")
		return err
	}
	if minersCnt == 0 {
		var start int32 = 0
		for {
			nodes, err := FetchMinersInfo(ctx, httpCli, minertrackerURL, start, 0, int64(nodeMgr.minerTrackBatchSize))
			if err != nil {
				entry.WithError(err).Errorf("fetch miners info: %s, from %d limit %d, timestamp %d", minertrackerURL, start, nodeMgr.minerTrackBatchSize, 0)
				time.Sleep(time.Duration(nodeMgr.retryTimeDelay) * time.Millisecond)
				continue
			}
			if len(nodes) == 0 {
				break
			}
			start = nodes[len(nodes)-1].ID
			for _, node := range nodes {
				err = nodeMgr.syncNode(ctx, node, excludeAddrPrefix)
				if err != nil {
					entry.WithError(err).Errorf("insert miner info: %d", node.ID)
				}
			}
		}
		_, err = collectionProgress.InsertOne(ctx, &TrackProgress{ID: 99999, Start: time.Now().Unix(), Timestamp: time.Now().Unix()})
		if err != nil {
			entry.WithError(err).Error("insert miner tracking progress")
		}
	}
	go func() {
		entry.Info("continuing tracking miners...")
		for {
			beginTime := time.Now().UnixNano()
			record := new(TrackProgress)
			err := collectionProgress.FindOne(ctx, bson.M{"_id": 99999}).Decode(record)
			if err != nil {
				if err == mongo.ErrNoDocuments {
					time.Now().Unix()
					record = &TrackProgress{ID: 99999, Start: time.Now().Unix() - int64(nodeMgr.avaliableNodeTimeGap)*60, Timestamp: time.Now().Unix()}
					_, err := collectionProgress.InsertOne(ctx, record)
					if err != nil {
						entry.WithError(err).Error("insert miner tracking progress")
						time.Sleep(time.Duration(nodeMgr.retryTimeDelay) * time.Millisecond)
						continue
					}
				} else {
					entry.WithError(err).Error("finding miner tracking progress")
					time.Sleep(time.Duration(nodeMgr.retryTimeDelay) * time.Millisecond)
					continue
				}
			}
			var start int32 = 0
			for {
				nodes, err := FetchMinersInfo(ctx, httpCli, minertrackerURL, start, record.Start, int64(nodeMgr.minerTrackBatchSize))
				if err != nil {
					entry.WithError(err).Errorf("fetch miners info: %s, from %d limit %d, timestamp %d", minertrackerURL, start, nodeMgr.minerTrackBatchSize, record.Start)
					time.Sleep(time.Duration(nodeMgr.retryTimeDelay) * time.Millisecond)
					continue
				}
				if len(nodes) == 0 {
					break
				}
				start = nodes[len(nodes)-1].ID
				for _, node := range nodes {
					err = nodeMgr.syncNode(ctx, node, excludeAddrPrefix)
					if err != nil {
						entry.WithError(err).Errorf("insert miner info: %d", node.ID)
					}
				}
			}
			collectionProgress.UpdateOne(ctx, bson.M{"_id": 99999}, bson.M{"$set": bson.M{"start": time.Now().Unix(), "timestamp": time.Now().Unix()}})
			sleepTime := time.Now().UnixNano() - beginTime
			if int64(nodeMgr.minerTrackInterval)*60*1000000000-sleepTime > 0 {
				time.Sleep(time.Duration(int64(nodeMgr.minerTrackInterval)*60*1000000000-sleepTime) * time.Nanosecond)
			}
		}
	}()
	return nil
}

func FetchMinersInfo(ctx context.Context, httpCli *http.Client, minertrackerURL string, from int32, timestamp, limit int64) ([]*Node, error) {
	entry := log.WithFields(log.Fields{Function: "fetchMinersInfo"})
	body := bytes.NewBuffer([]byte(fmt.Sprintf("{\"_id\":{\"$gt\":%d},\"timestamp\":{\"$gt\":%d}}", from, timestamp)))
	request, err := http.NewRequest("POST", fmt.Sprintf("%s/query?sort=_id&asc=true&limit=%d", minertrackerURL, limit), body)
	if err != nil {
		entry.WithError(err).Errorf("create request failed: %s, from %d limit %d, timestamp %d", minertrackerURL, from, limit, timestamp)
		return nil, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Add("Accept-Encoding", "gzip")
	resp, err := httpCli.Do(request)
	if err != nil {
		entry.WithError(err).Errorf("get miner logs failed: %s, from %d limit %d, timestamp %d", minertrackerURL, from, limit, timestamp)
		return nil, err
	}
	defer resp.Body.Close()
	reader := io.Reader(resp.Body)
	if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
		gbuf, err := gzip.NewReader(reader)
		if err != nil {
			entry.WithError(err).Errorf("decompress response body: %s, from %d limit %d, timestamp %d", minertrackerURL, from, limit, timestamp)
			return nil, err
		}
		reader = io.Reader(gbuf)
		defer gbuf.Close()
	}
	response := make([]*Node, 0)
	err = json.NewDecoder(reader).Decode(&response)
	if err != nil {
		entry.WithError(err).Errorf("decode miners failed: %s, from %d limit %d, timestamp %d", minertrackerURL, from, limit, timestamp)
		return nil, err
	}
	entry.Debugf("fetched %d miners", len(response))
	return response, nil
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
	nodeMgr.rwlock.Lock()
	node := nodeMgr.Nodes[id]
	nodeMgr.rwlock.Unlock()
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
	nodeMgr.rwlock2.Lock()
	item := nodeMgr.Tasks[node.ID]
	nodeMgr.rwlock2.Unlock()
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
	// otherDoc := bson.A{}
	// if node.Ext != "" && node.Ext[0] == '[' && node.Ext[len(node.Ext)-1] == ']' {
	// 	var bdoc interface{}
	// 	err := bson.UnmarshalExtJSON([]byte(node.Ext), true, &bdoc)
	// 	if err != nil {
	// 		entry.WithError(err).Warn("parse ext document")
	// 	} else {
	// 		otherDoc, _ = bdoc.(bson.A)
	// 	}
	// }
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
	cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx}
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
	if len(urls) == 0 {
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
