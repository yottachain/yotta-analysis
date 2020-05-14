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

	"github.com/eoscanada/eos-go"
	proto "github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	pbh "github.com/yottachain/P2PHost/pb"
	pb "github.com/yottachain/yotta-analysis/pb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
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
	collection := analyser.SelectYottaDB(minerID).Collection(PoolWeightTab)
	result := new(PoolWeight)
	err := collection.FindOne(context.Background(), bson.M{"_id": poolOwner}).Decode(result)
	if err != nil {
		entry.WithError(err).Error("fetching pool weight")
		return false
	}
	if result.PoolTotalCount == 0 {
		entry.Debug("total count of miners is 0")
		return false
	}
	threshold := float64(analyser.Params.ErrorNodePercentThreshold) / 100
	value := float64(result.PoolTotalCount-result.PoolErrorCount) / float64(result.PoolTotalCount)
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
	err := analyser.SelectYottaDB(spr.NID).Collection(NodeTab).FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(node)
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
	entry.Debug("checking VNI")
	b, err := analyser.CheckVNI(node, spr)
	if err != nil {
		entry.WithError(err).Warn("checking vni")
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
				entry.Info("rechecking vni successful")
			}
			return
		}
		// errstr := ""
		// if err != nil {
		// 	errstr = err.Error()
		// }
		entry.Info("rechecking vni failed, checking 100 more VNIs")
		i := 0
		var errCount int64 = 0
		flag := true
		for range [100]byte{} {
			i++
			spr.VNI, err = analyser.getRandomVNI(node.ID)
			if err != nil {
				entry.WithError(err).Errorf("get random vni%d failed", i)
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
			entry.Debugf("generated random vni%d: %s", i, spr.VNI)
			b, err := analyser.CheckVNI(node, spr)
			if err != nil {
				flag = false
				entry.WithError(err).Debugf("checking vni%d: %s", i, spr.VNI)
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
				entry.WithError(err).Debugf("checking vni%d successful: %s", i, spr.VNI)
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
			entry.Infof("finished 100 VNIs check, %d shards errors in %d checks", errCount, i)
			if errCount == 100 {
				entry.Warnf("100/100 random VNIs checking failed, punish %d%% deposit", analyser.Params.PunishPhase3Percent)
				if analyser.Params.PunishPhase3Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						left, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
						if err != nil {
							entry.WithError(err).Errorf("punishing %d%% deposit", analyser.Params.PunishPhase3Percent)
						} else if left == 0 {
							entry.Warn("no deposit can be punished")
						} else {
							entry.Infof("punished %d%% deposit", analyser.Params.PunishPhase3Percent)
						}
						defer func() {
							_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
							if err != nil {
								entry.WithError(err).Error("updating task status to 2")
							} else {
								entry.Debug("task status is updated to 2")
							}
						}()
					}
				}
			} else {
				entry.Warnf("%d/100 random VNIs checking failed, punish %d%% deposit", errCount, analyser.Params.PunishPhase1Percent)
				if analyser.Params.PunishPhase1Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
						if err != nil {
							entry.WithError(err).Errorf("punishing %d%% deposit", analyser.Params.PunishPhase1Percent)
						} else if left == 0 {
							entry.Warn("no deposit can be punished")
						} else {
							entry.Infof("punished %d%% deposit", analyser.Params.PunishPhase1Percent)
						}
					}
				}
			}
			return
		}
	}

	if scNode.ErrorCount == analyser.Params.PunishPhase1 {
		entry.Infof("phase 1: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase1Percent)
		if analyser.Params.PunishPhase1Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
				if err != nil {
					entry.WithError(err).Errorf("phase 1: punishing %d%% deposit", analyser.Params.PunishPhase1Percent)
				} else if left == 0 {
					entry.Warn("phase 1: no deposit can be punished")
				} else {
					entry.Infof("phase 1: punished %d%% deposit", analyser.Params.PunishPhase1Percent)
				}
			}
		}
	} else if scNode.ErrorCount == analyser.Params.PunishPhase2 {
		entry.Infof("phase 2: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase2Percent)
		if analyser.Params.PunishPhase2Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase2Percent))
				if err != nil {
					entry.WithError(err).Errorf("phase 2: punishing %d%% deposit", analyser.Params.PunishPhase2Percent)
				} else if left == 0 {
					entry.Warn("phase 2: no deposit can be punished")
				} else {
					entry.Infof("phase 2: punished %d%% deposit", analyser.Params.PunishPhase2Percent)
				}
			}
		}
	} else if scNode.ErrorCount == analyser.Params.PunishPhase3 {
		entry.Infof("phase 3: miner is offline for %d times in spotchecking, punishing %d%% deposit", scNode.ErrorCount, analyser.Params.PunishPhase3Percent)
		if analyser.Params.PunishPhase3Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
				if err != nil {
					entry.WithError(err).Errorf("phase 3: punishing %d%% deposit", analyser.Params.PunishPhase3Percent)
				} else if left == 0 {
					entry.Warn("phase 3: no deposit can be punished")
				} else {
					entry.Infof("phase 3: punished %d%% deposit", analyser.Params.PunishPhase3Percent)
				}
				defer func() {
					_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
					if err != nil {
						entry.WithError(err).Error("updating status of spotcheck miner to 2")
					} else {
						entry.Info("updated status of spotcheck miner to 2")
					}
				}()
			}
		}
	}
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
		entry.WithError(err).Error("unmarshaling VNI failed")
		return false, err
	}
	entry.Debug("VNI unmarshalled")
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

func (analyser *Analyser) punish(node *Node, percent int64) (int64, error) {
	entry := log.WithFields(log.Fields{Function: "punish", MinerID: node.ID})
	entry.Debugf("punishing %d deposit", percent)
	pledgeData, err := analyser.eostx.GetPledgeData(uint64(node.ID))
	if err != nil {
		entry.WithError(err).Error("get pledge data failed")
		return 0, err
	}
	entry.Debugf("get pledge data: %d/%d", int64(pledgeData.Deposit.Amount), int64(pledgeData.Total.Amount))
	totalAsset := pledgeData.Total
	leftAsset := pledgeData.Deposit
	punishAsset := pledgeData.Deposit
	if leftAsset.Amount == 0 {
		entry.Debug("no left deposit")
		return 0, nil
	}
	var retLeft int64 = 0
	punishFee := int64(totalAsset.Amount) * percent / 100
	if punishFee < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishFee)
		retLeft = int64(leftAsset.Amount - punishAsset.Amount)
	}
	entry.Debugf("punishing %f YTA", float64(punishFee)/10000)
	err = analyser.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		entry.WithError(err).Errorf("punishing %f YTA", float64(punishFee)/10000)
		return 0, err
	}
	entry.Infof("punished %f YTA", float64(punishFee)/10000)
	return retLeft, nil
}

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

//getRandomVNI find one VNI by miner ID and index of DNI table
func (analyser *Analyser) getRandomVNI(id int32) (string, error) {
	entry := log.WithFields(log.Fields{Function: "getRandomVNI", MinerID: id})
	collection := analyser.SelectYottaDB(id).Collection(DNITab)
	startTime := analyser.Params.SpotCheckSkipTime
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	if startTime == 0 {
		opt.Sort = bson.M{"_id": 1}
		firstDNI := new(DNI)
		cur0, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
		if err != nil {
			entry.WithError(err).Error("find first VNI failed")
			return "", fmt.Errorf("find first VNI failed: %s", err.Error())
		}
		defer cur0.Close(context.Background())
		if cur0.Next(context.Background()) {
			err := cur0.Decode(firstDNI)
			if err != nil {
				entry.WithError(err).Error("decoding first VNI failed")
				return "", fmt.Errorf("error when decoding first VNI: %s", err.Error())
			}
			entry.Debugf("found start VNI: %s -> %s", firstDNI.ID.Hex(), hex.EncodeToString(firstDNI.Shard.Data))
		} else {
			entry.Error("cannot find first VNI")
			return "", fmt.Errorf("cannot find first VNI")
		}
		startTime = firstDNI.ID.Timestamp().Unix()
	}
	entry.Debugf("start time of spotcheck time range is %d", startTime)
	opt.Sort = bson.M{"_id": -1}
	lastDNI := new(DNI)
	cur1, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
	if err != nil {
		entry.WithError(err).Error("find last VNI failed")
		return "", fmt.Errorf("find last VNI failed: %s", err.Error())
	}
	defer cur1.Close(context.Background())
	if cur1.Next(context.Background()) {
		err := cur1.Decode(lastDNI)
		if err != nil {
			entry.WithError(err).Error("decoding last VNI failed")
			return "", fmt.Errorf("error when decoding last DNI: %s", err.Error())
		}
		entry.Debugf("found end VNI: %s -> %s", lastDNI.ID.Hex(), hex.EncodeToString(lastDNI.Shard.Data))
	} else {
		entry.Error("cannot find last VNI")
		return "", fmt.Errorf("cannot find last DNI")
	}
	endTime := lastDNI.ID.Timestamp().Unix()
	entry.Debugf("end time of spotcheck time range is %d", endTime)
	if startTime >= endTime {
		entry.Error("start time is bigger than end time, no valid VNIs can be spotchecked")
		return "", fmt.Errorf("no valid VNIs can be spotchecked")
	}
	delta := rand.Int63n(endTime - startTime)
	selectedTime := startTime + delta
	selectedID := primitive.NewObjectIDFromTimestamp(time.Unix(selectedTime, 0))
	selectedDNI := new(DNI)
	cur2, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0, "_id": bson.M{"$gte": selectedID}}, &opt)
	if err != nil {
		entry.WithError(err).Error("finding random VNI")
		return "", fmt.Errorf("find random VNI failed: %s", err.Error())
	}
	defer cur2.Close(context.Background())
	if cur2.Next(context.Background()) {
		err := cur2.Decode(selectedDNI)
		if err != nil {
			entry.WithError(err).Error("decoding random VNI")
			return "", fmt.Errorf("error when decoding random VNI: %s", err.Error())
		}
		entry.Debugf("found random VNI: %s -> %s", selectedDNI.ID.Hex(), hex.EncodeToString(selectedDNI.Shard.Data))
	} else {
		entry.Error("cannot find random VNI")
		return "", fmt.Errorf("cannot find random VNI")
	}
	return base64.StdEncoding.EncodeToString(selectedDNI.Shard.Data), nil
}

//GetSpotCheckList creates a spotcheck task
func (analyser *Analyser) GetSpotCheckList() (*SpotCheckList, error) {
	entry := log.WithFields(log.Fields{Function: "GetSpotCheckList"})
	collectionS := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.analysisdbClient.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	for range [10]byte{} {
		spotCheckList := new(SpotCheckList)
		now := time.Now().Unix()
		snID := rand.Int31n(int32(analyser.SnCount))
		entry.Debugf("selected SN %d", snID)
		collectionN := analyser.SelectYottaDB(snID).Collection(NodeTab)
		//select spotchecked miner
		node := new(Node)
		total, err := collectionN.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1})
		if err != nil {
			entry.WithError(err).Errorf("calculating total count of spotcheckable miners in SN %d", snID)
			continue
		}
		if total == 0 {
			entry.Warnf("total count of spotcheckable miners in SN %d is 0", snID)
			continue
		}
		entry.Debugf("total count of spotcheckable miners in SN %d is %d", snID, total)
		n := rand.Intn(int(total))
		optionf := new(options.FindOptions)
		skip := int64(n)
		limit := int64(1)
		optionf.Limit = &limit
		optionf.Skip = &skip
		cur, err := collectionN.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1}, optionf)
		if err != nil {
			entry.WithError(err).Errorf("finding random miner for spotcheck in SN %d", snID)
			continue
		}
		if cur.Next(context.Background()) {
			err := cur.Decode(node)
			if err != nil {
				entry.WithError(err).Errorf("decoding spotcheck miner in SN %d", snID)
				cur.Close(context.Background())
				continue
			}
			entry.Infof("miner %d is to be spotchecked in SN %d", node.ID, snID)
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
		entry.Debugf("selecting random VNI of miner %d", node.ID)
		spotCheckTask.VNI, err = analyser.getRandomVNI(node.ID)
		if err != nil {
			entry.WithError(err).Errorf("selecting random VNI of miner %d", node.ID)
			continue
		}
		entry.Infof("selected random VNI of miner %d: %s", node.ID, spotCheckTask.VNI)
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
	collection := analyser.SelectYottaDB(0).Collection(NodeTab)
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
