package ytanalysis

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//TaskManager spotcheck task manager
type TaskManager struct {
	syncDBClient       *mongo.Client
	SpotCheckStartTime int64
	SpotCheckEndTime   int64
}

//NewTaskManager create a new task manager instance
func NewTaskManager(syncdbCli *mongo.Client, spotCheckStartTime, spotCheckEndTime int64) *TaskManager {
	return &TaskManager{syncDBClient: syncdbCli, SpotCheckStartTime: spotCheckStartTime, SpotCheckEndTime: spotCheckEndTime}
}

//FirstShard find first shard of miner
func (taskMgr *TaskManager) FirstShard(ctx context.Context, minerID int32) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "FirstShard", MinerID: minerID})
	collection := taskMgr.syncDBClient.Database(MetaDB).Collection(Shards)
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	opt.Sort = bson.M{"_id": 1}
	firstShard := new(Shard)
	cur, err := collection.Find(ctx, bson.M{"nodeId": minerID}, &opt)
	if err != nil {
		entry.WithError(err).Error("find first shard failed")
		return -1, fmt.Errorf("find first shard failed: %s", err.Error())
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		err := cur.Decode(firstShard)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				entry.Warnf("no shards found")
				return -1, nil
			}
			entry.WithError(err).Error("decoding first shard failed")
			return -1, fmt.Errorf("error when decoding first shard: %s", err.Error())
		}
		entry.Debugf("found start shard: %d -> %s", firstShard.ID, hex.EncodeToString(firstShard.VHF.Data))
		return firstShard.ID, nil
	}
	entry.Error("cannot find first shard")
	return -1, fmt.Errorf("cannot find first shard")
}

//randomVNI get a random VNI from one miner
func (taskMgr *TaskManager) randomVNI(ctx context.Context, node *Node) (string, error) {
	entry := log.WithFields(log.Fields{Function: "RandomVNI", MinerID: node.ID})
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randtag := r.Int31()
	st := time.Now().UnixNano()
	defer func() {
		entry.Tracef("<time trace %d>cost time total: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	}()
	collection := taskMgr.syncDBClient.Database(MetaDB).Collection(Shards)
	startTime := taskMgr.SpotCheckStartTime
	endTime := taskMgr.SpotCheckEndTime
	start64 := Int64ToBytes(node.FirstShard)
	sTime := int64(BytesToInt32(start64[0:4]))
	if sTime >= startTime {
		startTime = sTime
	}
	entry.Tracef("start time of spotcheck time range is %d", startTime)
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	opt.Sort = bson.M{"_id": -1}
	lastShard := new(Shard)
	cur1, err := collection.Find(ctx, bson.M{"nodeId": node.ID}, &opt)
	entry.Tracef("<time trace %d>cost time 2: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Error("find last shard failed")
		return "", fmt.Errorf("find last shard failed: %s", err.Error())
	}
	defer cur1.Close(ctx)
	if cur1.Next(ctx) {
		err := cur1.Decode(lastShard)
		if err != nil {
			entry.WithError(err).Error("decoding last shard failed")
			return "", fmt.Errorf("error when decoding last shard: %s", err.Error())
		}
		entry.Tracef("found last shard: %d -> %s", lastShard.ID, hex.EncodeToString(lastShard.VHF.Data))
	} else {
		entry.Error("cannot find last shard")
		return "", fmt.Errorf("cannot find last shard")
	}
	end64 := Int64ToBytes(lastShard.ID)
	eTime := int64(BytesToInt32(end64[0:4]))
	if eTime <= endTime || endTime == 0 {
		endTime = eTime
	}
	entry.Tracef("end time of spotcheck time range is %d", endTime)
	if startTime >= endTime {
		entry.Error("start time is bigger than end time, no valid shards can be spotchecked")
		return "", fmt.Errorf("no valid shards can be spotchecked")
	}
	opt.Sort = bson.M{"_id": 1}
	delta := rand.Int63n(endTime - startTime)
	selectedTime := startTime + delta
	sel32 := Int32ToBytes(int32(selectedTime))
	rand32 := rand.Uint32()
	sel32 = append(sel32, Uint32ToBytes(rand32)...)
	selectedID := BytesToInt64(sel32)
	selectedShard := new(Shard)
	cur2, err := collection.Find(ctx, bson.M{"nodeId": node.ID, "_id": bson.M{"$gte": selectedID}}, &opt)
	entry.Tracef("<time trace %d>cost time 3: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Error("finding random shard")
		return "", fmt.Errorf("find random shard failed: %s", err.Error())
	}
	defer cur2.Close(ctx)
	if cur2.Next(ctx) {
		err := cur2.Decode(selectedShard)
		if err != nil {
			entry.WithError(err).Error("decoding random shard")
			return "", fmt.Errorf("error when decoding random shard: %s", err.Error())
		}
		entry.Tracef("found random shard: %d -> %s", selectedShard.ID, hex.EncodeToString(selectedShard.VHF.Data))
	} else {
		entry.Error("cannot find random shard")
		return "", fmt.Errorf("cannot find random shard")
	}
	return base64.StdEncoding.EncodeToString(append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, selectedShard.VHF.Data...)), nil
}

//SpotCheckItem Spotcheck item
type SpotCheckItem struct {
	MinerID     int32    `bson:"nid"`
	CheckShard  string   `bson:"checkShard"`
	ExtraShards []string `bson:"extraShards"`
}

//ErrNoShardForSpotCheck throws when no shards found
var ErrNoShardForSpotCheck = errors.New("no shards for spotchecking")

//GetSpotCheckItem get one spotcheck item
func (taskMgr *TaskManager) GetSpotCheckItem(ctx context.Context, node *Node) (*SpotCheckItem, error) {
	entry := log.WithFields(log.Fields{Function: "GetSpotCheckItem", MinerID: node.ID})
	start := time.Now().UnixNano()
	checkShard, err := taskMgr.randomVNI(ctx, node)
	if err != nil {
		entry.WithError(err).Error("find spotcheck shard")
		return nil, err
	}
	if checkShard == "" {
		entry.Warn("no spotcheck shard found")
		return nil, ErrNoShardForSpotCheck
	}
	extraShards := make([]string, 0)
	var innerErr *error
	var wg sync.WaitGroup
	wg.Add(100)
	var lock sync.Mutex
	for i := 0; i < 100; i++ {
		go func() {
			defer wg.Done()
			s, err := taskMgr.randomVNI(ctx, node)
			if err != nil {
				innerErr = &err
				return
			}
			lock.Lock()
			defer lock.Unlock()
			extraShards = append(extraShards, s)
		}()
	}
	wg.Wait()
	if innerErr != nil {
		entry.WithError(*innerErr).Error("get spotcheck item")
		return nil, *innerErr
	}
	entry.Debugf("create spotcheck item cost %dms", (time.Now().UnixNano()-start)/1000000)
	return &SpotCheckItem{MinerID: node.ID, CheckShard: checkShard, ExtraShards: extraShards}, nil
}

//InsertSpotCheckItem insert spotcheck item to database
// func (taskMgr *TaskManager) InsertSpotCheckItem(ctx context.Context, item *SpotCheckItem) error {
// 	entry := log.WithFields(log.Fields{Function: "InsertSpotCheckItem", MinerID: item.MinerID})
// 	collection := taskMgr.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckItemTab)
// 	opts := new(options.UpdateOptions)
// 	upsert := true
// 	opts.Upsert = &upsert
// 	cond := bson.M{"checkShard": item.CheckShard, "extraShards": item.ExtraShards}
// 	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": item.MinerID}, bson.M{"$set": cond}, opts)
// 	if err != nil {
// 		entry.WithError(err).Error("insert spotcheck item")
// 		return err
// 	}
// 	return nil
// }
