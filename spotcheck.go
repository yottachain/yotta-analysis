package ytanalysis

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"strings"
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

func init() {
	rand.Seed(time.Now().UnixNano())
}

//StartRecheck starting recheck process
func (analyser *Analyser) StartRecheck() {
	entry := log.WithFields(log.Fields{Function: "StartRecheck"})
	entry.Info("starting deprecated task reaper...")
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	_, err := collectionS.UpdateMany(context.Background(), bson.M{"status": 1}, bson.M{"$set": bson.M{"status": 0}})
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
			_, err := collectionS.DeleteMany(context.Background(), bson.M{"timestamp": bson.M{"$lt": now - 3600*24}})
			if err != nil {
				entry.WithError(err).Warn("deleting expired spotcheck tasks")
			}
			entry.Debug("deleted expired spotcheck tasks")
		}
	}()
}

func (analyser *Analyser) ifNeedPunish(minerID int32, poolOwner string) bool {
	entry := log.WithFields(log.Fields{Function: "ifNeedPunish", MinerID: minerID, PoolOwner: poolOwner})
	collection := analyser.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	errTime := time.Now().Unix() - int64(analyser.Params.PoolErrorMinerTimeThreshold) //int64(spotcheckInterval)*60 - int64(punishPhase1)*punishGapUnit
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"poolOwner", poolOwner}, {"status", bson.D{{"$lt", 3}}}}}},
		{{"$project", bson.D{{"poolOwner", 1}, {"err", bson.D{{"$cond", bson.D{{"if", bson.D{{"$or", bson.A{bson.D{{"$eq", bson.A{"$status", 2}}}, bson.D{{"$lt", bson.A{"$timestamp", errTime}}}}}}}, {"then", 1}, {"else", 0}}}}}}}},
		{{"$group", bson.D{{"_id", "$poolOwner"}, {"poolTotalCount", bson.D{{"$sum", 1}}}, {"poolErrorCount", bson.D{{"$sum", "$err"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		entry.WithError(err).Warnf("aggregating miner count of pool owner %s", poolOwner)
		return false
	}
	defer cur.Close(context.Background())
	pw := new(PoolWeight)
	if cur.Next(context.Background()) {
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

func (analyser *Analyser) checkDataNode(spr *SpotCheckRecord) {
	entry := log.WithFields(log.Fields{Function: "checkDataNode", TaskID: spr.TaskID, MinerID: spr.NID, ShardHash: spr.VNI})
	entry.Info("task is under rechecking")
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	node := new(Node)
	err := analyser.analysisdbClient.Database(AnalysisDB).Collection(NodeTab).FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(node)
	if err != nil {
		entry.WithError(err).Error("decoding miner info which performing rechecking")
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 0")
			} else {
				entry.Debug("task status is updated to 0")
			}
		}()
		return
	}
	entry.Debug("decoded miner info")
	scNode := new(Node)
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	entry.Debug("checking shard")
	b, err := analyser.CheckVNI(node, spr)
	if err != nil {
		entry.WithError(err).Warn("checking shard")
		err = collectionSN.FindOneAndUpdate(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
		if err != nil {
			entry.WithError(err).Error("increasing error count of miner")
			defer func() {
				_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
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
			errCount = analyser.Params.PunishPhase3
		}
		if analyser.ifNeedPunish(node.ID, node.PoolOwner) {
			analyser.punish(0, node.ID, errCount, true)
		} else {
			analyser.punish(0, node.ID, errCount, false)
		}
		entry.Debug("increasing error count of miner")
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 2")
			} else {
				entry.Debug("task status is updated to 2")
			}
		}()
	} else {
		if b {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
			if err != nil {
				entry.WithError(err).Error("updating task status to 0")
			} else {
				entry.Info("rechecking shard successful")
			}
			analyser.punish(int32(0), node.ID, int32(0), false)
			return
		}
		entry.Info("rechecking shard failed, checking 100 more shards")
		i := 0
		var errCount int64 = 0
		flag := true
		for range [100]byte{} {
			i++
			spr.VNI, err = analyser.getRandomVNI(node.ID)
			if err != nil {
				entry.WithError(err).Errorf("get random shard %d failed", i)
				defer func() {
					_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
					if err != nil {
						entry.WithError(err).Error("updating task status to 0")
					} else {
						entry.Debug("task status is updated to 0")
					}
				}()
				return
			}
			entry.Debugf("generated random shard %d: %s", i, spr.VNI)
			b, err := analyser.CheckVNI(node, spr)
			if err != nil {
				flag = false
				entry.WithError(err).Debugf("checking shard %d: %s", i, spr.VNI)
				err = collectionSN.FindOneAndUpdate(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
				if err != nil {
					entry.WithError(err).Error("increasing error count of miner")
					defer func() {
						_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
						if err != nil {
							entry.WithError(err).Error("updating task status to 0")
						} else {
							entry.Debug("task status is updated to 0")
						}
					}()
					return
				}
				entry.Debug("increasing error count of miner")
				break
			}
			if !b {
				errCount++
				entry.WithError(err).Debugf("checking shard %d successful: %s", i, spr.VNI)
			}
		}
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
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
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						analyser.punish(2, node.ID, analyser.Params.PunishPhase3Percent, true)
						// left, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
						// if err != nil {
						// 	entry.WithError(err).Errorf("punishing %d%% deposit", analyser.Params.PunishPhase3Percent)
						// } else if left == 0 {
						// 	entry.Warn("no deposit can be punished")
						// } else {
						// 	entry.Infof("punished %d%% deposit", analyser.Params.PunishPhase3Percent)
						// }
						// defer func() {
						// 	_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
						// 	if err != nil {
						// 		entry.WithError(err).Error("updating task status to 2")
						// 	} else {
						// 		entry.Debug("task status is updated to 2")
						// 	}
						// }()
					}
				}
			} else {
				entry.Warnf("%d/100 random shards checking failed, punish %d%% deposit", errCount, analyser.Params.PunishPhase1Percent)
				if analyser.Params.PunishPhase1Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						analyser.punish(1, node.ID, analyser.Params.PunishPhase1Percent, true)
						// left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
						// if err != nil {
						// 	entry.WithError(err).Errorf("punishing %d%% deposit", analyser.Params.PunishPhase1Percent)
						// } else if left == 0 {
						// 	entry.Warn("no deposit can be punished")
						// } else {
						// 	entry.Infof("punished %d%% deposit", analyser.Params.PunishPhase1Percent)
						// }
					}
				}
			}
			return
		}
	}

	// if scNode.ErrorCount == analyser.Params.PunishPhase1 {
	// 	entry.Infof("phase 1: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase1Percent)
	// 	if analyser.Params.PunishPhase1Percent > 0 {
	// 		if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
	// 			left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
	// 			if err != nil {
	// 				entry.WithError(err).Errorf("phase 1: punishing %d%% deposit", analyser.Params.PunishPhase1Percent)
	// 			} else if left == 0 {
	// 				entry.Warn("phase 1: no deposit can be punished")
	// 			} else {
	// 				entry.Infof("phase 1: punished %d%% deposit", analyser.Params.PunishPhase1Percent)
	// 			}
	// 		}
	// 	}
	// } else if scNode.ErrorCount == analyser.Params.PunishPhase2 {
	// 	entry.Infof("phase 2: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase2Percent)
	// 	if analyser.Params.PunishPhase2Percent > 0 {
	// 		if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
	// 			left, err := analyser.punish(node, int64(analyser.Params.PunishPhase2Percent))
	// 			if err != nil {
	// 				entry.WithError(err).Errorf("phase 2: punishing %d%% deposit", analyser.Params.PunishPhase2Percent)
	// 			} else if left == 0 {
	// 				entry.Warn("phase 2: no deposit can be punished")
	// 			} else {
	// 				entry.Infof("phase 2: punished %d%% deposit", analyser.Params.PunishPhase2Percent)
	// 			}
	// 		}
	// 	}
	// } else if scNode.ErrorCount == analyser.Params.PunishPhase3 {
	// 	entry.Infof("phase 3: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase3Percent)
	// 	if analyser.Params.PunishPhase3Percent > 0 {
	// 		if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
	// 			left, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
	// 			if err != nil {
	// 				entry.WithError(err).Errorf("phase 3: punishing %d%% deposit", analyser.Params.PunishPhase3Percent)
	// 			} else if left == 0 {
	// 				entry.Warn("phase 3: no deposit can be punished")
	// 			} else {
	// 				entry.Infof("phase 3: punished %d%% deposit", analyser.Params.PunishPhase3Percent)
	// 			}
	// 			defer func() {
	// 				_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
	// 				if err != nil {
	// 					entry.WithError(err).Error("updating status of spotcheck miner to 2")
	// 				} else {
	// 					entry.Info("updated status of spotcheck miner to 2")
	// 				}
	// 			}()
	// 		}
	// 	}
	// }
}

//CheckVNI check whether vni is correct
func (analyser *Analyser) CheckVNI(node *Node, spr *SpotCheckRecord) (bool, error) {
	entry := log.WithFields(log.Fields{Function: "CheckVNI", TaskID: spr.TaskID, MinerID: spr.NID, ShardHash: spr.VNI})
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle()
	entry.Debugf("setting connect timeout to %d", analyser.Params.SpotCheckConnectTimeout)
	req := &pbh.ConnectReq{Id: node.NodeID, Addrs: node.Addrs}
	_, err := analyser.checker.lhost.Connect(ctx, req)
	if err != nil {
		entry.WithError(err).Error("connecting spotchecked miner failed")
		return false, err
	}
	entry.Debug("spotchecked miner connected")
	defer analyser.checker.lhost.DisConnect(context.Background(), &pbh.StringMsg{Value: node.NodeID})

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
	ctx2, cancle2 := context.WithTimeout(context.Background(), time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle2()
	shardData, err := analyser.checker.SendMsg(ctx2, node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		entry.WithError(err).Error("sending rechecking command failed")
		return false, err
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

func (analyser *Analyser) punish(msgType, minerID, count int32, needPunish bool) {
	entry := log.WithFields(log.Fields{Function: "punish", MinerID: minerID})
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
	snID := int(minerID) % len(analyser.mqClis)
	ret := analyser.mqClis[snID].Send(fmt.Sprintf("sn%d", snID), append([]byte{byte(PunishMessage)}, b...))
	if !ret {
		entry.Warnf("sending PunishMessage of miner %d failed", minerID)
	}
}

// func (analyser *Analyser) punish(node *Node, percent int64) (int64, error) {
// 	entry := log.WithFields(log.Fields{Function: "punish", MinerID: node.ID})
// 	entry.Debugf("punishing %d deposit", percent)
// 	pledgeData, err := analyser.eostx.GetPledgeData(uint64(node.ID))
// 	if err != nil {
// 		entry.WithError(err).Error("get pledge data failed")
// 		return 0, err
// 	}
// 	entry.Debugf("get pledge data: %d/%d", int64(pledgeData.Deposit.Amount), int64(pledgeData.Total.Amount))
// 	totalAsset := pledgeData.Total
// 	leftAsset := pledgeData.Deposit
// 	punishAsset := pledgeData.Deposit
// 	if leftAsset.Amount == 0 {
// 		entry.Debug("no left deposit")
// 		return 0, nil
// 	}
// 	var retLeft int64 = 0
// 	punishFee := int64(totalAsset.Amount) * percent / 100
// 	if punishFee < int64(punishAsset.Amount) {
// 		punishAsset.Amount = eos.Int64(punishFee)
// 		retLeft = int64(leftAsset.Amount - punishAsset.Amount)
// 	}
// 	entry.Debugf("punishing %f YTA", float64(punishFee)/10000)
// 	err = analyser.eostx.DeducePledge(uint64(node.ID), &punishAsset)
// 	if err != nil {
// 		entry.WithError(err).Errorf("punishing %f YTA", float64(punishFee)/10000)
// 		return 0, err
// 	}
// 	entry.Infof("punished %f YTA", float64(punishFee)/10000)
// 	return retLeft, nil
// }

//UpdateTaskStatus process error task of spotchecking
func (analyser *Analyser) UpdateTaskStatus(taskID string, invalidNode int32) error {
	entry := log.WithFields(log.Fields{Function: "UpdateTaskStatus", TaskID: taskID, MinerID: invalidNode})
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)

	//first task can be rechecked
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	sprn := new(SpotCheckRecord)
	err := collectionS.FindOneAndUpdate(context.Background(), bson.M{"_id": taskID}, bson.M{"$inc": bson.M{"dup": 1}}, opts).Decode(sprn)
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
	cur, err := collectionS.Find(context.Background(), bson.M{"nid": invalidNode}, opt)
	if err != nil {
		entry.WithError(err).Errorf("fetching lastest spotcheck task")
		return err
	}
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
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
	cur, err = collectionS.Find(context.Background(), bson.M{"nid": invalidNode}, opt)
	if err != nil {
		entry.WithError(err).Errorf("fetching last spotcheck task")
		return err
	}
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(lastSpotCheck)
		if err != nil {
			entry.WithError(err).Errorf("decoding last spotcheck task")
			return err
		}
		entry.Debug("last spotcheck task fetched")
		if lastSpotCheck.Status == 0 {
			_, err = collectionSN.UpdateOne(context.Background(), bson.M{"_id": invalidNode}, bson.M{"$set": bson.M{"errorCount": 0}})
			if err != nil {
				entry.WithError(err).Error("updating error count to 0")
				return err
			}
			entry.Debug("error count is updated to 0")
		}
	}
	//update task status to 1(rechecking)
	_, err = collectionS.UpdateOne(context.Background(), bson.M{"_id": taskID}, bson.M{"$set": bson.M{"status": 1}})
	if err != nil {
		entry.WithError(err).Error("updating status of spotcheck task to 1")
		return err
	}
	entry.Debug("status of spotcheck task is updated to 1")
	//rechecking
	entry.Debug("add rechecking task to executing pool")
	analyser.pool.JobQueue <- func() {
		analyser.checkDataNode(lastestSpotCheck)
	}
	return nil
}

//getRandomVNI find one VNI by miner ID and index of shards table
func (analyser *Analyser) getRandomVNI(id int32) (string, error) {
	entry := log.WithFields(log.Fields{Function: "getRandomVNI", MinerID: id})
	collection := analyser.analysisdbClient.Database(MetaDB).Collection(Shards)
	startTime := analyser.Params.SpotCheckSkipTime
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	if startTime == 0 {
		opt.Sort = bson.M{"_id": 1}
		firstShard := new(Shard)
		cur0, err := collection.Find(context.Background(), bson.M{"nodeId": id}, &opt)
		if err != nil {
			entry.WithError(err).Error("find first shard failed")
			return "", fmt.Errorf("find first shard failed: %s", err.Error())
		}
		defer cur0.Close(context.Background())
		if cur0.Next(context.Background()) {
			err := cur0.Decode(firstShard)
			if err != nil {
				entry.WithError(err).Error("decoding first shard failed")
				return "", fmt.Errorf("error when decoding first shard: %s", err.Error())
			}
			entry.Debugf("found start shard: %d -> %s", firstShard.ID, hex.EncodeToString(firstShard.VHF.Data))
		} else {
			entry.Error("cannot find first shard")
			return "", fmt.Errorf("cannot find first shard")
		}
		start64 := Int64ToBytes(firstShard.ID)
		startTime = int64(BytesToInt32(start64[0:4]))
		// startTime = firstShard.ID.Timestamp().Unix()
	}
	entry.Debugf("start time of spotcheck time range is %d", startTime)
	opt.Sort = bson.M{"_id": -1}
	lastShard := new(Shard)
	cur1, err := collection.Find(context.Background(), bson.M{"nodeId": id}, &opt)
	if err != nil {
		entry.WithError(err).Error("find last shard failed")
		return "", fmt.Errorf("find last shard failed: %s", err.Error())
	}
	defer cur1.Close(context.Background())
	if cur1.Next(context.Background()) {
		err := cur1.Decode(lastShard)
		if err != nil {
			entry.WithError(err).Error("decoding last shard failed")
			return "", fmt.Errorf("error when decoding last shard: %s", err.Error())
		}
		entry.Debugf("found end shard: %d -> %s", lastShard.ID, hex.EncodeToString(lastShard.VHF.Data))
	} else {
		entry.Error("cannot find last shard")
		return "", fmt.Errorf("cannot find last shard")
	}
	end64 := Int64ToBytes(lastShard.ID)
	endTime := int64(BytesToInt32(end64[0:4]))
	// endTime := lastShard.ID.Timestamp().Unix()
	entry.Debugf("end time of spotcheck time range is %d", endTime)
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
	// selectedID := primitive.NewObjectIDFromTimestamp(time.Unix(selectedTime, 0))
	selectedShard := new(Shard)
	cur2, err := collection.Find(context.Background(), bson.M{"nodeId": id, "_id": bson.M{"$gte": selectedID}}, &opt)
	if err != nil {
		entry.WithError(err).Error("finding random shard")
		return "", fmt.Errorf("find random shard failed: %s", err.Error())
	}
	defer cur2.Close(context.Background())
	if cur2.Next(context.Background()) {
		err := cur2.Decode(selectedShard)
		if err != nil {
			entry.WithError(err).Error("decoding random shard")
			return "", fmt.Errorf("error when decoding random shard: %s", err.Error())
		}
		entry.Debugf("found random shard: %d -> %s", selectedShard.ID, hex.EncodeToString(selectedShard.VHF.Data))
	} else {
		entry.Error("cannot find random shard")
		return "", fmt.Errorf("cannot find random shard")
	}
	return base64.StdEncoding.EncodeToString(append([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}, selectedShard.VHF.Data...)), nil
}

//GetSpotCheckList creates a spotcheck task
func (analyser *Analyser) GetSpotCheckList() (*SpotCheckList, error) {
	entry := log.WithFields(log.Fields{Function: "GetSpotCheckList"})
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	for range [10]byte{} {
		spotCheckList := new(SpotCheckList)
		now := time.Now().Unix()
		// snID := rand.Int31n(int32(analyser.SnCount))
		// entry.Debugf("selected SN %d", snID)
		collectionN := analyser.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)

		total, err := collectionN.CountDocuments(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1})
		if err != nil {
			entry.WithError(err).Error("calculating total count of spotcheckable miners")
			continue
		}
		if total == 0 {
			entry.Warn("total count of spotcheckable miners is 0")
			continue
		}
		entry.Debugf("total count of spotcheckable miners is %d", total)
		n := rand.Intn(int(total))
		optionf := new(options.FindOptions)
		skip := int64(n)
		limit := int64(1)
		optionf.Limit = &limit
		optionf.Skip = &skip
		cur, err := collectionN.Find(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1}, optionf)
		if err != nil {
			entry.WithError(err).Errorf("finding random miner for spotchecking")
			continue
		}
		//select spotchecked miner
		node := new(Node)
		if cur.Next(context.Background()) {
			err := cur.Decode(node)
			if err != nil {
				entry.WithError(err).Error("decoding spotcheck miner")
				cur.Close(context.Background())
				continue
			}
			entry.Infof("miner %d is to be spotchecked", node.ID)
		}
		cur.Close(context.Background())
		//check spotcheck node
		scNode := new(Node)
		err = collectionSN.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(scNode)
		if err == nil && scNode.Status == 2 {
			entry.Warnf("miner %d is under rebuilding", scNode.ID)
			continue
		}
		//check lastest spotcheck
		lastSpotCheck := new(SpotCheckRecord)
		optionf = new(options.FindOptions)
		optionf.Sort = bson.M{"timestamp": -1}
		optionf.Limit = &limit
		cur, err = collectionS.Find(context.Background(), bson.M{"nid": node.ID}, optionf)
		if err != nil {
			entry.WithError(err).Errorf("fetching lastest spotcheck task of miner %d", node.ID)
			continue
		}
		if cur.Next(context.Background()) {
			err := cur.Decode(lastSpotCheck)
			if err != nil {
				entry.WithError(err).Errorf("decoding lastest spotcheck task of miner %d", node.ID)
				cur.Close(context.Background())
				continue
			}
			entry.Debugf("found lastest spotcheck task of miner %d: %s", node.ID, lastSpotCheck.TaskID)
			if (now-lastSpotCheck.Timestamp < analyser.Params.SpotCheckInterval*60/2) || lastSpotCheck.Status == 1 {
				entry.Warnf("conflict with lastest spotcheck task of miner %d: %s", node.ID, lastSpotCheck.TaskID)
				cur.Close(context.Background())
				continue
			}
		}

		cur.Close(context.Background())
		//select random shardspotCheckTask := new(SpotCheckTask)
		spotCheckTask := new(SpotCheckTask)
		spotCheckTask.ID = node.ID
		spotCheckTask.NodeID = node.NodeID
		addr := GetRelayURL(node.Addrs)
		if addr != "" {
			spotCheckTask.Addr = addr
		} else {
			spotCheckTask.Addr = analyser.CheckPublicAddr(node.Addrs)
		}
		entry.Debugf("selecting random shard of miner %d", node.ID)
		spotCheckTask.VNI, err = analyser.getRandomVNI(node.ID)
		if err != nil {
			entry.WithError(err).Errorf("selecting random shard of miner %d", node.ID)
			continue
		}
		entry.Infof("selected random shard of miner %d: %s", node.ID, spotCheckTask.VNI)
		spotCheckList.TaskID = primitive.NewObjectID()
		spotCheckList.Timestamp = now
		spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)

		spr := &SpotCheckRecord{TaskID: spotCheckList.TaskID.Hex(), NID: spotCheckTask.ID, VNI: spotCheckTask.VNI, Status: 0, Timestamp: spotCheckList.Timestamp, Dup: 0}
		_, err = collectionS.InsertOne(context.Background(), spr)
		if err != nil {
			entry.WithError(err).Errorf("inserting spotcheck record of miner %d", node.ID)
			continue
		}
		node.ErrorCount = 0
		_, err = collectionSN.InsertOne(context.Background(), node)
		if err != nil {
			errstr := err.Error()
			if !strings.ContainsAny(errstr, "duplicate key error") {
				entry.WithError(err).Errorf("inserting miner %d to SpotCheckNode table", node.ID)
				continue
			}
		}
		return spotCheckList, nil
	}
	entry.Warn("no miners can be spotchecked")
	return nil, errors.New("no miners can be spotchecked")
}

//IsNodeSelected check if node is selected for spotchecking
func (analyser *Analyser) IsNodeSelected() (bool, error) {
	entry := log.WithFields(log.Fields{Function: "IsNodeSelected"})
	collection := analyser.analysisdbClient.Database(AnalysisDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "status": 1, "version": bson.M{"$gte": analyser.Params.MinerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheckable miners")
		return false, errors.New("error when get count of spotcheckable miners")
	}
	entry.Debugf("count of spotcheckable miners is %d", c)
	d, err := collection.CountDocuments(context.Background(), bson.M{"status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*analyser.Params.AvaliableNodeTimeGap}, "version": bson.M{"$gte": analyser.Params.MinerVersionThreshold}})
	if err != nil {
		entry.WithError(err).Error("calculating count of spotcheck-executing miners")
		return false, errors.New("error when get count of spotcheck-executing miners")
	}
	entry.Debugf("count of spotcheck-executing miners is %d", d)
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
