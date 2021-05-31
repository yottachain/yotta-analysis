package ytanalysis

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pbh "github.com/yottachain/P2PHost/pb"
	pb "github.com/yottachain/yotta-analysis/pbanalysis"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var EmptyShardError = errors.New("downloading shard response is empty")

func init() {
	rand.Seed(time.Now().UnixNano())
}

//StartRecheck starting recheck process
func (analyser *Analyser) StartRecheck(ctx context.Context) {
	entry := log.WithFields(log.Fields{Function: "StartRecheck"})
	entry.Info("starting deprecated task reaper...")
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	_, err := collectionS.UpdateMany(ctx, bson.M{"status": 1}, bson.M{"$set": bson.M{"status": 0}})
	if err != nil {
		entry.WithError(err).Error("updating spotcheck tasks with status 1 to 0")
	} else {
		entry.Info("update tasks status of 1 to 0")
	}
	go func() {
		for {
			entry.Debug("reaper process sleep for 1 hour")
			time.Sleep(time.Duration(1) * time.Hour)
			now := time.Now().Unix()
			_, err := collectionS.DeleteMany(ctx, bson.M{"timestamp": bson.M{"$lt": now - 3600*24}})
			if err != nil {
				entry.WithError(err).Warn("deleting expired spotcheck tasks")
			}
			entry.Debug("deleted expired spotcheck tasks")
		}
	}()
}

func (analyser *Analyser) ifNeedPunish(ctx context.Context, minerID int32, poolOwner string) bool {
	entry := log.WithFields(log.Fields{Function: "ifNeedPunish", MinerID: minerID, PoolOwner: poolOwner})
	collection := analyser.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	errTime := time.Now().Unix() - int64(analyser.Params.PoolErrorMinerTimeThreshold) //int64(spotcheckInterval)*60 - int64(punishPhase1)*punishGapUnit
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"poolOwner", poolOwner}, {"status", bson.D{{"$lt", 3}}}}}},
		{{"$project", bson.D{{"poolOwner", 1}, {"err", bson.D{{"$cond", bson.D{{"if", bson.D{{"$or", bson.A{bson.D{{"$eq", bson.A{"$status", 2}}}, bson.D{{"$lt", bson.A{"$timestamp", errTime}}}}}}}, {"then", 1}, {"else", 0}}}}}}}},
		{{"$group", bson.D{{"_id", "$poolOwner"}, {"poolTotalCount", bson.D{{"$sum", 1}}}, {"poolErrorCount", bson.D{{"$sum", "$err"}}}}}},
	}
	cur, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		entry.WithError(err).Warnf("aggregating miner count of pool owner %s", poolOwner)
		return false
	}
	defer cur.Close(ctx)
	pw := new(PoolWeight)
	if cur.Next(ctx) {
		err := cur.Decode(pw)
		if err != nil {
			entry.WithError(err).Warnf("decoding miner count of pool owner %s", poolOwner)
			return false
		}
	}
	threshold := float64(analyser.Params.ErrorNodePercentThreshold) / 100
	if pw.PoolTotalCount == 0 {
		return false
	}
	value := float64(pw.PoolTotalCount-pw.PoolErrorCount) / float64(pw.PoolTotalCount)
	entry.Debugf("avaliable miner percent is %f%%, current threshold is %d%%", value*100, analyser.Params.ErrorNodePercentThreshold)
	if value < threshold {
		return true
	}
	return false
}

func (analyser *Analyser) checkDataNode(ctx context.Context, node *Node, spr *SpotCheckRecord) {
	entry := log.WithFields(log.Fields{Function: "checkDataNode", TaskID: spr.TaskID, MinerID: spr.NID, ShardHash: spr.VNI})
	entry.Info("task is under rechecking")
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	scNode := new(Node)
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	entry.Debug("checking shard")
	b, err := analyser.CheckVNI(ctx, node, spr)
	if err != nil {
		entry.WithError(err).Warn("checking shard")
		err = collectionSN.FindOneAndUpdate(ctx, bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
		if err != nil {
			entry.WithError(err).Error("increasing error count of miner")
			defer func() {
				_, err := collectionS.UpdateOne(ctx, bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
				if err != nil {
					entry.WithError(err).Error("updating task status to 0")
				} else {
					entry.Debug("task status is updated to 0")
				}
			}()
			return
		}
		errCount := scNode.ErrorCount
		if errCount > analyser.Params.PunishPhase3 {
			_, err := collectionSN.UpdateOne(ctx, bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"errorCount": analyser.Params.PunishPhase3}})
			if err != nil {
				entry.WithError(err).Errorf("updating error count to punish phase 3: %d", analyser.Params.PunishPhase3)
			}
			errCount = analyser.Params.PunishPhase3
		}
		if analyser.Params.PunishPhase1Percent != 0 && analyser.Params.PunishPhase2Percent != 0 && analyser.Params.PunishPhase3Percent != 0 {
			if analyser.ifNeedPunish(ctx, node.ID, node.PoolOwner) {
				analyser.punish(ctx, 0, node.ID, errCount, true)
			} else {
				analyser.punish(ctx, 0, node.ID, errCount, false)
			}
		}
		entry.Debugf("increasing error count of miner: %d", errCount)
		defer func() {
			_, err := collectionS.UpdateOne(ctx, bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 2")
			} else {
				entry.Debug("task status is updated to 2")
			}
		}()
	} else {
		if b {
			_, err := collectionS.UpdateOne(ctx, bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 0")
			} else {
				entry.Info("rechecking shard successful")
			}
			analyser.punish(ctx, int32(0), node.ID, int32(0), false)
			return
		}
		entry.Info("rechecking shard failed, checking 100 more shards")
		i := 0
		var errCount int64 = 0
		flag := true
		for j := 0; i < 100; j++ {
			i++
			spr.VNI = spr.ExtraShards[j]
			entry.Debugf("generated random shard %d: %s", i, spr.VNI)
			b, err := analyser.CheckVNI(ctx, node, spr)
			if err != nil {
				flag = false
				entry.WithError(err).Debugf("checking shard %d: %s", i, spr.VNI)
				err = collectionSN.FindOneAndUpdate(ctx, bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
				if err != nil {
					entry.WithError(err).Error("increasing error count of miner")
					defer func() {
						_, err := collectionS.UpdateOne(ctx, bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
						if err != nil {
							entry.WithError(err).Error("updating task status to 0")
						} else {
							entry.Debug("task status is updated to 0")
						}
					}()
					return
				}
				errCount2 := scNode.ErrorCount
				if errCount2 > analyser.Params.PunishPhase3 {
					_, err := collectionSN.UpdateOne(ctx, bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"errorCount": analyser.Params.PunishPhase3}})
					if err != nil {
						entry.WithError(err).Errorf("updating error count to punish phase 3: %d", analyser.Params.PunishPhase3)
					}
					errCount2 = analyser.Params.PunishPhase3
				}
				if analyser.Params.PunishPhase1Percent != 0 && analyser.Params.PunishPhase2Percent != 0 && analyser.Params.PunishPhase3Percent != 0 {
					if analyser.ifNeedPunish(ctx, node.ID, node.PoolOwner) {
						analyser.punish(ctx, 0, node.ID, errCount2, true)
					} else {
						analyser.punish(ctx, 0, node.ID, errCount2, false)
					}
				}
				entry.Debugf("increasing error count of miner: %d", errCount2)
				break
			}
			if !b {
				errCount++
				entry.Debugf("checking shard %d successful: %s", i, spr.VNI)
			} else {
				entry.Debugf("checking shard %d failed: %s", i, spr.VNI)
			}
		}
		defer func() {
			_, err := collectionS.UpdateOne(ctx, bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 2")
			} else {
				entry.Debug("task status is updated to 2")
			}
		}()
		if flag {
			entry.Infof("finished 100 shards check, %d shards errors in %d checks", errCount, i)
			if errCount == 100 {
				entry.Warnf("100/100 random shards checking failed, punish %d%% deposit", analyser.Params.PunishPhase3Percent)
				if analyser.Params.PunishPhase3Percent > 0 {
					if analyser.ifNeedPunish(ctx, spr.NID, node.PoolOwner) {
						analyser.punish(ctx, 2, node.ID, analyser.Params.PunishPhase3Percent, true)
					}
				}
			} else {
				entry.Warnf("%d/100 random shards checking failed, punish %d%% deposit", errCount, analyser.Params.PunishPhase1Percent)
				if analyser.Params.PunishPhase1Percent > 0 {
					if analyser.ifNeedPunish(ctx, spr.NID, node.PoolOwner) {
						analyser.punish(ctx, 1, node.ID, analyser.Params.PunishPhase1Percent, true)
					}
				}
			}
			return
		}
	}
}

//CheckVNI check whether vni is correct
func (analyser *Analyser) CheckVNI(ctx context.Context, node *Node, spr *SpotCheckRecord) (bool, error) {
	entry := log.WithFields(log.Fields{Function: "CheckVNI", TaskID: spr.TaskID, MinerID: spr.NID, ShardHash: spr.VNI})
	ctx, cancle := context.WithTimeout(ctx, time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle()
	entry.Debugf("setting connect timeout to %d, connecting %v", analyser.Params.SpotCheckConnectTimeout, node.Addrs)
	req := &pbh.ConnectReq{Id: node.NodeID, Addrs: node.Addrs}
	_, err := analyser.checker.lhost.Connect(ctx, req)
	if err != nil {
		entry.WithError(err).Error("connecting spotchecked miner failed")
		return false, err
	}
	entry.Debug("spotchecked miner connected")
	defer analyser.checker.lhost.DisConnect(ctx, &pbh.StringMsg{Value: node.NodeID})

	rawvni, err := base64.StdEncoding.DecodeString(spr.VNI)
	if err != nil {
		entry.WithError(err).Error("unmarshaling shard failed")
		return false, err
	}
	entry.Debug("shard unmarshalled")
	rawvni = rawvni[len(rawvni)-16:]
	downloadRequest := &pb.DownloadShardRequest{VHF: rawvni}
	checkData, err := proto.Marshal(downloadRequest)
	if err != nil {
		entry.WithError(err).Error("marshalling protobuf message")
		return false, err
	}
	entry.Debug("protobuf message marshalled")
	entry.Debugf("setting message-sending timeout to %d", analyser.Params.SpotCheckConnectTimeout)
	ctx2, cancle2 := context.WithTimeout(ctx, time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle2()
	shardData, err := analyser.checker.SendMsg(ctx2, node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		if strings.ContainsAny(err.Error(), "YTFS: data not found") {
			return false, nil
		}
		entry.WithError(err).Error("sending rechecking command failed")
		return false, err
	}
	if len(shardData) == 0 {
		entry.WithError(err).Error("downloading shard response is empty")
		return false, EmptyShardError
	}
	entry.Debug("rechecking command is sent to spotcheck miner")
	var share pb.DownloadShardResponse
	err = proto.Unmarshal(shardData[2:], &share)
	if err != nil {
		entry.WithError(err).Error("unmarshalling rechecking response failed")
		return false, err
	}
	entry.Debugf("rechecking response unmarshalled: %s", hex.EncodeToString(shardData))
	if downloadRequest.VerifyVHF(share.Data) {
		entry.Debug("shard is correct")
		return true, nil
	}
	entry.Debug("shard is incorrect")
	return false, nil
}

func (analyser *Analyser) punish(ctx context.Context, msgType, minerID, count int32, needPunish bool) {
	entry := log.WithFields(log.Fields{Function: "punish", MinerID: minerID})
	if analyser.Params.PunishPhase1Percent == 0 && analyser.Params.PunishPhase2Percent == 0 && analyser.Params.PunishPhase3Percent == 0 {
		entry.Infof("skip sending PunishMessage of miner %d, message type %d, need punishment %v to SN: %d", minerID, msgType, needPunish, count)
		return
	}
	entry.Infof("send PunishMessage of miner %d, message type %d, need punishment %v: %d", minerID, msgType, needPunish, count)
	rule := make(map[int32]int32)
	rule[analyser.Params.PunishPhase1] = analyser.Params.PunishPhase1Percent
	rule[analyser.Params.PunishPhase2] = analyser.Params.PunishPhase2Percent
	rule[analyser.Params.PunishPhase3] = analyser.Params.PunishPhase3Percent
	msg := &pb.PunishMessage{NodeID: minerID, Type: msgType, Count: count, NeedPunish: needPunish, Rule: rule}
	b, err := proto.Marshal(msg)
	if err != nil {
		entry.WithError(err).Error("marshaling PunishMessage failed")
		return
	}
	snID := int(minerID) % len(analyser.nodeMgr.MqClis)
	ret := analyser.nodeMgr.MqClis[snID].Send(ctx, fmt.Sprintf("sn%d", snID), append([]byte{byte(PunishMessage)}, b...))
	if !ret {
		entry.Warnf("sending PunishMessage of miner %d failed", minerID)
	}
}

//UpdateTaskStatus process error task of spotchecking
func (analyser *Analyser) UpdateTaskStatus(ctx context.Context, taskID string, invalidNode int32) error {
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus", TaskID: taskID, MinerID: invalidNode})
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randtag := r.Int31()
	st := time.Now().UnixNano()
	defer func() {
		entry.Debugf("<time trace %d>cost time total: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	}()
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)

	analyser.nodeMgr.rwlock.Lock()
	node := analyser.nodeMgr.Nodes[invalidNode]
	if node == nil {
		analyser.nodeMgr.rwlock.Unlock()
		return fmt.Errorf("node is invalid")
	}
	analyser.nodeMgr.rwlock.Unlock()

	//first task can be rechecked
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	sprn := new(SpotCheckRecord)
	err := collectionS.FindOneAndUpdate(ctx, bson.M{"_id": taskID}, bson.M{"$inc": bson.M{"dup": 1}}, opts).Decode(sprn)
	entry.Tracef("<time trace %d>cost time 1: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Errorf("increasing dup")
		return err
	}
	if sprn.Dup != 1 {
		entry.Debugf("dup is %d, skip rechecking", sprn.Dup)
		return nil
	}

	//check if lastest spotcheck task of miner invalidNode is task with taskID
	lastestSpotCheck := new(SpotCheckRecord)
	opt := new(options.FindOptions)
	var limit int64 = 1
	opt.Sort = bson.M{"timestamp": -1}
	opt.Limit = &limit
	cur, err := collectionS.Find(ctx, bson.M{"nid": invalidNode}, opt)
	entry.Tracef("<time trace %d>cost time 2: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Errorf("fetching lastest spotcheck task")
		return err
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		err := cur.Decode(lastestSpotCheck)
		if err != nil {
			entry.WithError(err).Errorf("decoding lastest spotcheck task")
			return err
		}
		entry.Debug("lastest spotcheck task fetched")
		if lastestSpotCheck.TaskID != taskID {
			entry.Warn("reported task is not matching lastest spotcheck task")
			return nil
		} else if lastestSpotCheck.Status != 0 {
			entry.Warn("reported task is under rechecking")
			return nil
		}
	}

	//check if status of last spotcheck is 0, then update error count of miner invalidNode to 0
	lastSpotCheck := new(SpotCheckRecord)
	opt = new(options.FindOptions)
	var skip int64 = 1
	opt.Sort = bson.M{"timestamp": -1}
	opt.Limit = &limit
	opt.Skip = &skip
	cur, err = collectionS.Find(ctx, bson.M{"nid": invalidNode}, opt)
	entry.Tracef("<time trace %d>cost time 3: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Errorf("fetching last spotcheck task")
		return err
	}
	defer cur.Close(ctx)
	if cur.Next(ctx) {
		err := cur.Decode(lastSpotCheck)
		if err != nil {
			entry.WithError(err).Errorf("decoding last spotcheck task")
			return err
		}
		entry.Debug("last spotcheck task fetched")
		if lastSpotCheck.Status == 0 {
			_, err = collectionSN.UpdateOne(ctx, bson.M{"_id": invalidNode}, bson.M{"$set": bson.M{"errorCount": 0}})
			entry.Tracef("<time trace %d>cost time 4: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
			if err != nil {
				entry.WithError(err).Error("updating error count to 0")
				return err
			}
			entry.Debug("error count is updated to 0")
		}
	}
	//update task status to 1(rechecking)
	_, err = collectionS.UpdateOne(ctx, bson.M{"_id": taskID}, bson.M{"$set": bson.M{"status": 1}})
	entry.Tracef("<time trace %d>cost time 5: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	if err != nil {
		entry.WithError(err).Error("updating status of spotcheck task to 1")
		return err
	}
	entry.Debug("status of spotcheck task is updated to 1")
	//rechecking
	entry.Debug("add rechecking task to executing pool")
	analyser.pool.JobQueue <- func() {
		analyser.checkDataNode(ctx, node, lastestSpotCheck)
	}
	entry.Tracef("<time trace %d>cost time 6: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	return nil
}

//GetSpotCheckList creates a spotcheck task
func (analyser *Analyser) GetSpotCheckList(ctx context.Context) (*SpotCheckList, error) {
	entry := log.WithFields(log.Fields{Function: "GetSpotCheckList"})
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	randtag := r.Int31()
	st := time.Now().UnixNano()
	defer func() {
		entry.Debugf("<time trace %d>cost time total: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
	}()
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	//collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	for range [10]byte{} {
		spotCheckList := new(SpotCheckList)
		now := time.Now().Unix()
		node, item, err := analyser.nodeMgr.GetSpotCheckTask(ctx)
		if err != nil {
			entry.WithError(err).Error("get spotcheck item failed")
			continue
		}
		entry.Tracef("<time trace %d>cost time 1: %dms", randtag, (time.Now().UnixNano()-st)/1000000)

		spotCheckTask := new(SpotCheckTask)
		spotCheckTask.ID = node.ID
		spotCheckTask.NodeID = node.NodeID
		addr := GetRelayURL(node.Addrs)
		if addr != "" {
			spotCheckTask.Addr = addr
		} else {
			spotCheckTask.Addr = analyser.CheckPublicAddr(node.Addrs)
		}
		entry.Debugf("selecting random shard of miner %d, address is %s", node.ID, spotCheckTask.Addr)
		spotCheckTask.VNI = item.CheckShard
		spotCheckList.TaskID = primitive.NewObjectID()
		spotCheckList.Timestamp = now
		spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)

		spr := &SpotCheckRecord{TaskID: spotCheckList.TaskID.Hex(), NID: spotCheckTask.ID, VNI: spotCheckTask.VNI, ExtraShards: item.ExtraShards, Status: 0, Timestamp: spotCheckList.Timestamp, Dup: 0}
		_, err = collectionS.InsertOne(ctx, spr)
		entry.Tracef("<time trace %d>cost time 2: %dms", randtag, (time.Now().UnixNano()-st)/1000000)
		if err != nil {
			entry.WithError(err).Errorf("inserting spotcheck record of miner %d", node.ID)
			continue
		}
		return spotCheckList, nil
	}
	entry.Warn("no miners can be spotchecked")
	return nil, errors.New("no miners can be spotchecked")
}

//IsNodeSelected check if node is selected for spotchecking
func (analyser *Analyser) IsNodeSelected() (bool, error) {
	entry := log.WithFields(log.Fields{Function: "IsNodeSelected"})
	c := atomic.LoadInt64(&analyser.nodeMgr.c)
	d := atomic.LoadInt64(&analyser.nodeMgr.d)
	if c == 0 || d == 0 {
		entry.Warn("count of spotcheckable miners or count of spotcheck-executing miners is 0")
		return false, nil
	}
	if float32(c)/float32(d) > 2 {
		entry.Debug("c/d>2, set c=2000, d=1000")
		c = 2000
		d = 1000
	}
	if d < 100 {
		entry.Debug("d<100, set c=c*100 d=d*100")
		c = c * 100
		d = d * 100
	}
	n := rand.Int63n(d * analyser.Params.SpotCheckInterval)
	entry.Debugf("random number is %d", n)
	if n < c {
		entry.Debug("miner selected")
		return true, nil
	}
	entry.Debug("miner unselected")
	return false, nil
}
