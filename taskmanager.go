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
	"github.com/tikv/client-go/rawkv"
)

//TaskManager spotcheck task manager
type TaskManager struct {
	tikvCli            *rawkv.Client
	SpotCheckStartTime int64
	SpotCheckEndTime   int64
}

//NewTaskManager create a new task manager instance
func NewTaskManager(tikvCli *rawkv.Client, spotCheckStartTime, spotCheckEndTime int64) *TaskManager {
	return &TaskManager{tikvCli: tikvCli, SpotCheckStartTime: spotCheckStartTime, SpotCheckEndTime: spotCheckEndTime}
}

//FirstShard find first shard of miner
func (taskMgr *TaskManager) FirstShard(ctx context.Context, minerID int32) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "FirstShard", MinerID: minerID})
	firstShard, err := FetchFirstNodeShard(ctx, taskMgr.tikvCli, minerID)
	if err != nil {
		if err == NoValError {
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
	lastShard, err := FetchLastNodeShard(ctx, taskMgr.tikvCli, node.ID)
	if err != nil {
		if err == NoValError {
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
	shards, err := FetchNodeShards(ctx, taskMgr.tikvCli, node.ID, selectedID, 9223372036854775807, 1)
	if err != nil {
		entry.WithError(err).Error("fetching random shard from tikv")
		return "", fmt.Errorf("error when fetching random shard from tikv: %s", err.Error())
	}
	if len(shards) == 0 {
		entry.Error("cannot find random shard")
		return "", fmt.Errorf("cannot find random shard")
	}
	entry.Tracef("<time trace %d>cost time 3: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	entry.Tracef("found random shard: %d -> %s", shards[0].ID, hex.EncodeToString(shards[0].VHF))
	return base64.StdEncoding.EncodeToString(append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, shards[0].VHF...)), nil
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

func FetchBlock(ctx context.Context, tikvCli *rawkv.Client, blockID int64) (*Block, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_BLOCKS, blockID)))
	if err != nil {
		return nil, err
	}
	block := new(Block)
	err = block.FillBytes(buf)
	if err != nil {
		return nil, err
	}
	return block, nil
}

func FetchShard(ctx context.Context, tikvCli *rawkv.Client, shardID int64) (*Shard, error) {
	buf, err := tikvCli.Get(ctx, []byte(fmt.Sprintf("%s_%019d", PFX_SHARDS, shardID)))
	if err != nil {
		return nil, err
	}
	shard := new(Shard)
	err = shard.FillBytes(buf)
	if err != nil {
		return nil, err
	}
	return shard, nil
}

func FetchNodeShards(ctx context.Context, tikvCli *rawkv.Client, nodeId int32, shardFrom, shardTo int64, limit int64) ([]*Shard, error) {
	entry := log.WithFields(log.Fields{Function: "FetchNodeShards", MinerID: nodeId, ShardID: shardFrom, "Limit": limit})
	batchSize := int64(10000)
	from := fmt.Sprintf("%019d", shardFrom)
	to := fmt.Sprintf("%019d", shardTo)
	shards := make([]*Shard, 0)
	cnt := int64(0)
	for {
		lmt := batchSize
		if cnt+batchSize-limit > 0 {
			lmt = limit - cnt
		}
		_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, from)), []byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, to)), int(lmt))
		if err != nil {
			return nil, err
		}
		if len(values) == 0 {
			break
		}
		for _, buf := range values {
			s := new(Shard)
			err := s.FillBytes(buf)
			if err != nil {
				return nil, err
			}
			if s.NodeID != nodeId && s.NodeID2 != nodeId {
				continue
			}
			s.NodeID = nodeId
			shards = append(shards, s)
		}
		from = fmt.Sprintf("%019d", shards[len(shards)-1].ID+1)
		cnt += int64(len(values))
	}
	entry.Debugf("fetch %d shards", len(shards))
	return shards, nil
}

func FetchShards(ctx context.Context, tikvCli *rawkv.Client, shardFrom int64, shardTo int64) ([]*Shard, error) {
	_, values, err := tikvCli.Scan(ctx, []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardFrom))), []byte(fmt.Sprintf("%s_%s", PFX_SHARDS, fmt.Sprintf("%019d", shardTo))), 164)
	if err != nil {
		return nil, err
	}
	shards := make([]*Shard, 0)
	for _, buf := range values {
		s := new(Shard)
		err := s.FillBytes(buf)
		if err != nil {
			return nil, err
		}
		shards = append(shards, s)
	}
	return shards, nil
}

func FetchFirstNodeShard(ctx context.Context, tikvCli *rawkv.Client, nodeId int32) (*Shard, error) {
	shards, err := FetchNodeShards(ctx, tikvCli, nodeId, 0, 9223372036854775807, 1)
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		return nil, NoValError
	}
	return shards[0], nil
}

func FetchLastNodeShard(ctx context.Context, tikvCli *rawkv.Client, nodeId int32) (*Shard, error) {
	from := fmt.Sprintf("%019d", 0)
	to := "9999999999999999999"
	_, values, err := tikvCli.ReverseScan(ctx, append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, to)), '\x00'), append([]byte(fmt.Sprintf("%s_%d_%s", PFX_SHARDNODES, nodeId, from)), '\x00'), 1)
	if err != nil {
		return nil, err
	}
	shards := make([]*Shard, 0)
	for _, buf := range values {
		s := new(Shard)
		err := s.FillBytes(buf)
		if err != nil {
			return nil, err
		}
		if s.NodeID != nodeId && s.NodeID2 != nodeId {
			continue
		}
		s.NodeID = nodeId
		shards = append(shards, s)
	}
	if len(shards) == 0 {
		return nil, NoValError
	}
	return shards[0], nil
}
