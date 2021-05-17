package ytanalysis

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	log "github.com/sirupsen/logrus"
)

//TaskManager spotcheck task manager
type TaskManager struct {
	syncDBClient       *sqlx.DB
	SpotCheckStartTime int64
	SpotCheckEndTime   int64
}

//NewTaskManager create a new task manager instance
func NewTaskManager(syncdbCli *sqlx.DB, spotCheckStartTime, spotCheckEndTime int64) *TaskManager {
	return &TaskManager{syncDBClient: syncdbCli, SpotCheckStartTime: spotCheckStartTime, SpotCheckEndTime: spotCheckEndTime}
}

//FirstShard find first shard of miner
func (taskMgr *TaskManager) FirstShard(ctx context.Context, minerID int32) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "FirstShard", MinerID: minerID})
	firstShard := new(Shard)
	err := taskMgr.syncDBClient.QueryRowxContext(ctx, "select * from shards where nid=? order by id asc limit 1", minerID).StructScan(firstShard)
	if err != nil {
		if err == sql.ErrNoRows {
			entry.Warnf("no shard found")
			return -1, nil
		}
		entry.WithError(err).Error("decoding first shard failed")
		return -1, fmt.Errorf("error when decoding first shard: %s", err.Error())
	}
	entry.Debugf("found start shard: %d -> %s", firstShard.ID, hex.EncodeToString(firstShard.VHF))
	return firstShard.ID, nil
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
	startTime := taskMgr.SpotCheckStartTime
	endTime := taskMgr.SpotCheckEndTime
	start64 := Int64ToBytes(node.FirstShard)
	sTime := int64(BytesToInt32(start64[0:4]))
	if sTime >= startTime {
		startTime = sTime
	}
	entry.Tracef("start time of spotcheck time range is %d", startTime)
	lastShard := new(Shard)
	err := taskMgr.syncDBClient.QueryRowxContext(ctx, "select * from shards where nid=? order by id desc limit 1", node.ID).StructScan(lastShard)
	if err != nil {
		if err == sql.ErrNoRows {
			entry.Error("cannot find last shard")
			return "", fmt.Errorf("cannot find last shard")
		}
		entry.WithError(err).Error("decoding last shard failed")
		return "", fmt.Errorf("error when decoding last shard: %s", err.Error())
	}
	entry.Tracef("found last shard: %d -> %s", lastShard.ID, hex.EncodeToString(lastShard.VHF))

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
	delta := rand.Int63n(endTime - startTime)
	selectedTime := startTime + delta
	sel32 := Int32ToBytes(int32(selectedTime))
	rand32 := rand.Uint32()
	sel32 = append(sel32, Uint32ToBytes(rand32)...)
	selectedID := BytesToInt64(sel32)
	selectedShard := new(Shard)
	err = taskMgr.syncDBClient.QueryRowxContext(ctx, "select * from shards where nid=? and id>=? order by id asc limit 1", node.ID, selectedID).StructScan(selectedShard)
	if err != nil {
		if err == sql.ErrNoRows {
			entry.Error("cannot find random shard")
			return "", fmt.Errorf("cannot find random shard")
		}
		entry.WithError(err).Error("decoding random shard")
		return "", fmt.Errorf("error when decoding random shard: %s", err.Error())
	}
	entry.Tracef("<time trace %d>cost time 3: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	entry.Tracef("found random shard: %d -> %s", selectedShard.ID, hex.EncodeToString(selectedShard.VHF))
	return base64.StdEncoding.EncodeToString(append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, selectedShard.VHF...)), nil
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
