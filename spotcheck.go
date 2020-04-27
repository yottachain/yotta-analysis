package ytanalysis

import (
	"context"
	"encoding/base64"
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
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

//StartRecheck starting recheck process
func (analyser *Analyser) StartRecheck() {
	log.Infof("ytanalysis: StartRecheck: starting deprecated task reaper...")
	collectionS := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	_, err := collectionS.UpdateMany(context.Background(), bson.M{"status": 1}, bson.M{"$set": bson.M{"status": 0}})
	if err != nil {
		log.Warnf("ytanalysis: StartRecheck: updating spotcheck tasks with status 1 to 0: %s", err.Error())
	}
	go func() {
		for {
			time.Sleep(time.Duration(1) * time.Hour)
			now := time.Now().Unix()
			_, err := collectionS.DeleteMany(context.Background(), bson.M{"timestamp": bson.M{"$lt": now - 3600*24}})
			if err != nil {
				log.Warnf("ytanalysis: StartRecheck: deleting spotcheck tasks with status 0: %s\n", err.Error())
			}
		}
	}()
}

func (analyser *Analyser) selectNodeTab(minerID int32) *mongo.Collection {
	return analyser.client.Database(DBName(analyser.SnCount, minerID)).Collection(NodeTab)
}

func (analyser *Analyser) ifNeedPunish(minerID int32, poolOwner string) bool {
	collection := analyser.client.Database(DBName(analyser.SnCount, minerID)).Collection(PoolWeightTab)
	result := new(PoolWeight)
	err := collection.FindOne(context.Background(), bson.M{"_id": poolOwner}).Decode(result)
	if err != nil {
		log.Errorf("ytanalysis: ifNeedPunish: getting pool weight of %s: %s", poolOwner, err.Error())
		return false
	}
	if result.PoolTotalCount == 0 {
		return false
	}
	threshold := float64(analyser.Params.ErrorNodePercentThreshold) / 100
	value := float64(result.PoolTotalCount-result.PoolErrorCount) / float64(result.PoolTotalCount)
	if value < threshold {
		return true
	}
	return false
}

func (analyser *Analyser) checkDataNode(spr *SpotCheckRecord) {
	log.Infof("ytanalysis: checkDataNode: SN rechecking task: %d -> %s -> %s", spr.NID, spr.TaskID, spr.VNI)
	collectionS := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.client.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	node := new(Node)
	err := analyser.selectNodeTab(spr.NID).FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(node)
	if err != nil {
		log.Errorf("ytanalysis: checkDataNode: decoding node which performing rechecking: %d -> %s -> %s", spr.NID, spr.TaskID, err.Error())
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: updating task %s status to 0: %s", spr.TaskID, err.Error())
			}
		}()
		return
	}
	scNode := new(Node)
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	b, err := analyser.CheckVNI(node, spr)
	if err != nil {
		log.Warnf("ytanalysis: checkDataNode: vni check error: %d -> %s -> %s: %s", spr.NID, spr.TaskID, spr.VNI, err.Error())
		err = collectionSN.FindOneAndUpdate(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
		if err != nil {
			log.Errorf("ytanalysis: checkDataNode: increasing error count of miner %d: %s", spr.NID, err.Error())
			defer func() {
				_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: updating task %s status to 0: %s", spr.TaskID, err.Error())
				}
			}()
			return
		}
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: updating task %s status to 2: %s", spr.TaskID, err.Error())
			}
		}()
	} else {
		if b {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: update status of task %s to 0: %s", spr.TaskID, err.Error())
			} else {
				log.Infof("ytanalysis: checkDataNode: vni check success: %d -> %s -> %s", spr.NID, spr.TaskID, spr.VNI)
			}
			return
		}
		errstr := ""
		if err != nil {
			errstr = err.Error()
		}
		log.Infof("ytanalysis: checkDataNode: vni check failed, checking 100 more VNIs: %d -> %s -> %s: %s", spr.NID, spr.TaskID, spr.VNI, errstr)
		i := 0
		var errCount int64 = 0
		flag := true
		for range [100]byte{} {
			i++
			spr.VNI, err = analyser.getRandomVNI(node.ID)
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: get random vni%d error: %d -> %s -> %s: %s", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
				defer func() {
					_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
					if err != nil {
						log.Errorf("ytanalysis: checkDataNode: error happens when update task %s status to 0: %s", spr.TaskID, err.Error())
					}
				}()
				return
			}
			b, err := analyser.CheckVNI(node, spr)
			if err != nil {
				flag = false
				log.Warnf("ytanalysis: checkDataNode: vni%d check error: %d -> %s -> %s: %s", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
				err = collectionSN.FindOneAndUpdate(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$inc": bson.M{"errorCount": 1}}, opts).Decode(scNode)
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: increasing error count of miner %d: %s", spr.NID, err.Error())
					defer func() {
						_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 0}})
						if err != nil {
							log.Errorf("ytanalysis: checkDataNode: error happens when update task %s status to 0: %s", spr.TaskID, err.Error())
						}
					}()
					return
				}
				break
			}
			if !b {
				errCount++
			}
		}
		defer func() {
			_, err := collectionS.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: updating task %s status to 2: %s", spr.TaskID, err.Error())
			}
		}()
		if flag {
			log.Infof("ytanalysis: checkDataNode: finished 100 VNIs check, %d verify errors in %d checks", errCount, i)
			if errCount == 100 {
				log.Warnf("ytanalysis: checkDataNode: 100/100 random VNI checking of miner %d has failed, punish %d%% deposit", spr.NID, analyser.Params.PunishPhase3Percent)
				if analyser.Params.PunishPhase3Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						_, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
						if err != nil {
							log.Errorf("ytanalysis: checkDataNode: punishing 50%% deposit of node %d: %s", spr.NID, err.Error())
						}
						defer func() {
							_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
							if err != nil {
								log.Errorf("ytanalysis: checkDataNode: updating status of spotcheck node %d to 2: %s", spr.NID, err.Error())
							}
						}()
					}
				}
			} else {
				log.Infof("ytanalysis: checkDataNode: %d/100 random VNI checking of miner %d has failed, punish %d%% deposit", errCount, spr.NID, analyser.Params.PunishPhase1Percent)
				if analyser.Params.PunishPhase1Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
						left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
						if err != nil {
							log.Errorf("ytanalysis: checkDataNode: punishing 1%% deposit of node %d: %s", spr.NID, err.Error())
						} else if left == 0 {
							log.Warnf("ytanalysis: checkDataNode: no deposit can be punished: %d", spr.NID)
						} else {
							log.Infof("ytanalysis: checkDataNode: do phase 1 punishment to miner %d", spr.NID)
						}
					}
				}
			}
			return
		}
	}

	if scNode.ErrorCount == analyser.Params.PunishPhase1 {
		log.Infof("ytanalysis: checkDataNode: miner %d has been offline for %d times in spotchecking, do phase 1 punishment", spr.NID, scNode.ErrorCount)
		if analyser.Params.PunishPhase1Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase1Percent))
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: punishing %d%% of miner %d: %s", analyser.Params.PunishPhase1Percent, spr.NID, err.Error())
				} else if left == 0 {
					log.Warnf("ytanalysis: checkDataNode: no deposit can be punished: %d", scNode.ID)
				} else {
					log.Infof("ytanalysis: checkDataNode: do phase 1 punishment to miner %d", scNode.ID)
				}
			}
		}
	} else if scNode.ErrorCount == analyser.Params.PunishPhase2 {
		log.Infof("ytanalysis: checkDataNode: miner %d has been offline for %d times in spotchecking, do phase 2 punishment", spr.NID, scNode.ErrorCount)
		if analyser.Params.PunishPhase2Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase2Percent))
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: punishing %d%% of miner %d: %s", analyser.Params.PunishPhase2Percent, spr.NID, err.Error())
				} else if left == 0 {
					log.Warnf("ytanalysis: checkDataNode: no deposit can be punished: %d", scNode.ID)
				} else {
					log.Infof("ytanalysis: checkDataNode: do phase 2 punishment to miner %d", scNode.ID)
				}
			}
		}
	} else if scNode.ErrorCount == analyser.Params.PunishPhase3 {
		log.Infof("ytanalysis: checkDataNode: miner %d has been offline for %d times in spotchecking, do phase 3 punishment", spr.NID, scNode.ErrorCount)
		if analyser.Params.PunishPhase3Percent > 0 {
			if analyser.ifNeedPunish(spr.NID, node.PoolOwner) {
				left, err := analyser.punish(node, int64(analyser.Params.PunishPhase3Percent))
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: punishing %d%% of miner %d: %s", analyser.Params.PunishPhase3Percent, spr.NID, err.Error())
				} else if left == 0 {
					log.Warnf("ytanalysis: checkDataNode: no deposit can be punished: %d", scNode.ID)
				} else {
					log.Infof("ytanalysis: checkDataNode: do phase 3 punishment to miner %d", scNode.ID)
				}
				defer func() {
					_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": spr.NID}, bson.M{"$set": bson.M{"status": 2}})
					if err != nil {
						log.Errorf("ytanalysis: checkDataNode: updating status of spotcheck node %d to 2: %s", spr.NID, err.Error())
					}
				}()
			}
		}
	}
}

//CheckVNI check whether vni is correct
func (analyser *Analyser) CheckVNI(node *Node, spr *SpotCheckRecord) (bool, error) {
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle()
	req := &pbh.ConnectReq{Id: node.NodeID, Addrs: node.Addrs}
	_, err := analyser.checker.lhost.Connect(ctx, req)
	if err != nil {
		return false, err
	}
	defer analyser.checker.lhost.DisConnect(context.Background(), &pbh.StringMsg{Value: node.NodeID})

	rawvni, err := base64.StdEncoding.DecodeString(spr.VNI)
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: unmarshaling VNI: %d %s %s", node.ID, spr.VNI, err.Error())
		return false, err
	}
	rawvni = rawvni[len(rawvni)-16:]
	downloadRequest := &pb.DownloadShardRequest{VHF: rawvni}
	checkData, err := proto.Marshal(downloadRequest)
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: marshalling protobuf message: downloadrequest: %d -> %s -> %s", node.ID, spr.VNI, err.Error())
		return false, err
	}
	ctx2, cancle2 := context.WithTimeout(context.Background(), time.Second*time.Duration(analyser.Params.SpotCheckConnectTimeout))
	defer cancle2()
	shardData, err := analyser.checker.SendMsg(ctx2, node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: SN send recheck command failed: %d -> %s -> %s", node.ID, spr.VNI, err.Error())
		return false, err
	}
	var share pb.DownloadShardResponse
	err = proto.Unmarshal(shardData[2:], &share)
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: SN unmarshal recheck response failed: %d -> %s -> %s", node.ID, spr.VNI, err.Error())
		return false, err
	}
	if downloadRequest.VerifyVHF(share.Data) {
		return true, nil
	}
	return false, nil
}

func (analyser *Analyser) punish(node *Node, percent int64) (int64, error) {
	pledgeData, err := analyser.eostx.GetPledgeData(uint64(node.ID))
	if err != nil {
		return 0, err
	}
	totalAsset := pledgeData.Total
	leftAsset := pledgeData.Deposit
	punishAsset := pledgeData.Deposit
	if leftAsset.Amount == 0 {
		return 0, nil
	}

	var retLeft int64 = 0
	punishFee := int64(totalAsset.Amount) * percent / 100
	if punishFee < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishFee)
		retLeft = int64(leftAsset.Amount - punishAsset.Amount)
	}
	err = analyser.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		return 0, err
	}
	log.Infof("ytanalysis: Punish: punish %d%% deposit of node %d: %d", percent, node.ID, punishFee)
	return retLeft, nil
}

//UpdateTaskStatus process error task of spotchecking
func (analyser *Analyser) UpdateTaskStatus(taskID string, invalidNode int32) error {
	collectionS := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.client.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	//check if lastest spotcheck task of miner invalidNode is task with taskID
	lastestSpotCheck := new(SpotCheckRecord)
	opt := new(options.FindOptions)
	var limit int64 = 1
	opt.Sort = bson.M{"timestamp": -1}
	opt.Limit = &limit
	cur, err := collectionS.Find(context.Background(), bson.M{"nid": invalidNode}, opt)
	if err != nil {
		log.Errorf("ytanalysis: UpdateTaskStatus: fetching lastest spotcheck task: %s", err.Error())
		return err
	}
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(lastestSpotCheck)
		if err != nil {
			log.Errorf("ytanalysis: UpdateTaskStatus: decoding lastest spotcheck task: %s", err.Error())
			return err
		}
		if lastestSpotCheck.TaskID != taskID {
			log.Warn("ytanalysis: UpdateTaskStatus: reported task is not lastest spotcheck task")
			return nil
		} else if lastestSpotCheck.Status != 0 {
			log.Warn("ytanalysis: UpdateTaskStatus: reported task is under rechecking")
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
		log.Errorf("ytanalysis: UpdateTaskStatus: fetching last spotcheck task: %s", err.Error())
		return err
	}
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(lastSpotCheck)
		if err != nil {
			log.Errorf("ytanalysis: UpdateTaskStatus: decoding last spotcheck task: %s", err.Error())
			return err
		}
		if lastSpotCheck.Status == 0 {
			_, err = collectionSN.UpdateOne(context.Background(), bson.M{"_id": invalidNode}, bson.M{"$set": bson.M{"errorCount": 0}})
			if err != nil {
				log.Errorf("ytanalysis: UpdateTaskStatus: update error count of miner %d to 0: %s", invalidNode, err.Error())
				return err
			}
		}
	}
	//update task status to 1(rechecking)
	_, err = collectionS.UpdateOne(context.Background(), bson.M{"_id": taskID}, bson.M{"$set": bson.M{"status": 1}})
	if err != nil {
		log.Errorf("ytanalysis: UpdateTaskStatus: update status of spotcheck task %s to 1: %s", taskID, err.Error())
		return err
	}
	//rechecking
	analyser.pool.JobQueue <- func() {
		analyser.checkDataNode(lastestSpotCheck)
	}
	return nil
}

//getRandomVNI find one VNI by miner ID and index of DNI table
func (analyser *Analyser) getRandomVNI(id int32) (string, error) {
	collection := analyser.client.Database(DBName(analyser.SnCount, id)).Collection(DNITab)
	startTime := analyser.Params.SpotCheckSkipTime
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	if startTime == 0 {
		opt.Sort = bson.M{"_id": 1}
		firstDNI := new(DNI)
		cur0, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
		if err != nil {
			return "", fmt.Errorf("find first VNI failed: %s", err.Error())
		}
		defer cur0.Close(context.Background())
		if cur0.Next(context.Background()) {
			err := cur0.Decode(firstDNI)
			if err != nil {
				return "", fmt.Errorf("error when decoding first DNI: %s", err.Error())
			}
		} else {
			return "", fmt.Errorf("cannot find first DNI")
		}
		startTime = firstDNI.ID.Timestamp().Unix()
	}
	opt.Sort = bson.M{"_id": -1}
	lastDNI := new(DNI)
	cur1, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
	if err != nil {
		return "", fmt.Errorf("find last VNI failed: %s", err.Error())
	}
	defer cur1.Close(context.Background())
	if cur1.Next(context.Background()) {
		err := cur1.Decode(lastDNI)
		if err != nil {
			return "", fmt.Errorf("error when decoding last DNI: %s", err.Error())
		}
	} else {
		return "", fmt.Errorf("cannot find last DNI")
	}
	endTime := lastDNI.ID.Timestamp().Unix()
	if startTime > endTime {
		return "", fmt.Errorf("no valid VNIs can be spotchecked")
	}
	delta := rand.Int63n(endTime - startTime)
	selectedTime := startTime + delta
	selectedID := primitive.NewObjectIDFromTimestamp(time.Unix(selectedTime, 0))
	selectedDNI := new(DNI)
	cur2, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0, "_id": bson.M{"$gte": selectedID}}, &opt)
	if err != nil {
		return "", fmt.Errorf("find random VNI failed: %s", err.Error())
	}
	defer cur2.Close(context.Background())
	if cur2.Next(context.Background()) {
		err := cur2.Decode(selectedDNI)
		if err != nil {
			return "", fmt.Errorf("error when decoding random DNI: %s", err.Error())
		}
	} else {
		return "", fmt.Errorf("cannot find random DNI")
	}
	return base64.StdEncoding.EncodeToString(selectedDNI.Shard.Data), nil
}

//GetSpotCheckList creates a spotcheck task
func (analyser *Analyser) GetSpotCheckList() (*SpotCheckList, error) {
	collectionS := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionSN := analyser.client.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	for range [10]byte{} {
		spotCheckList := new(SpotCheckList)
		now := time.Now().Unix()
		snID := rand.Int31n(int32(analyser.SnCount))
		collectionN := analyser.client.Database(DBName(analyser.SnCount, snID)).Collection(NodeTab)
		//select spotchecked miner
		node := new(Node)
		total, err := collectionN.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1})
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: calculating total count of spotcheckable nodes: %s", err.Error())
			continue
		}
		if total == 0 {
			log.Warn("ytanalysis: GetSpotCheckList: total count of spotcheckable nodes is zero")
			continue
		}
		n := rand.Intn(int(total))
		optionf := new(options.FindOptions)
		skip := int64(n)
		limit := int64(1)
		optionf.Limit = &limit
		optionf.Skip = &skip
		cur, err := collectionN.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1}, optionf)
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: fetching spotcheckable node: %s", err.Error())
			continue
		}
		if cur.Next(context.Background()) {
			err := cur.Decode(node)
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: decoding spotchecked node: %s", err.Error())
				cur.Close(context.Background())
				continue
			}
			log.Infof("ytanalysis: GetSpotCheckList: node %d will be spotchecked", node.ID)
		}
		cur.Close(context.Background())
		//check spotcheck node
		scNode := new(Node)
		err = collectionSN.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(scNode)
		if err == nil && scNode.Status == 2 {
			log.Warnf("miner %d is under rebuilding", scNode.ID)
			continue
		}
		//check last spotcheck
		lastSpotCheck := new(SpotCheckRecord)
		optionf = new(options.FindOptions)
		optionf.Sort = bson.M{"timestamp": -1}
		optionf.Limit = &limit
		cur, err = collectionS.Find(context.Background(), bson.M{"nid": node.ID}, optionf)
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: fetching last spotcheck task: %s", err.Error())
			continue
		}
		if cur.Next(context.Background()) {
			err := cur.Decode(lastSpotCheck)
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: decoding last spotcheck task: %s", err.Error())
				cur.Close(context.Background())
				continue
			}
			if (now-lastSpotCheck.Timestamp < analyser.Params.SpotCheckInterval*60/2) || lastSpotCheck.Status == 1 {
				log.Warnf("ytanalysis: GetSpotCheckList: conflict with last spotcheck task")
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
		spotCheckTask.VNI, err = analyser.getRandomVNI(node.ID)
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: selecting random vni of miner %d: %s", node.ID, err.Error())
			continue
		}
		log.Infof("ytanalysis: GetSpotCheckList: select random VNI for miner %d -> %s", node.ID, spotCheckTask.VNI)
		spotCheckList.TaskID = primitive.NewObjectID()
		spotCheckList.Timestamp = now
		spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)

		spr := &SpotCheckRecord{TaskID: spotCheckList.TaskID.Hex(), NID: spotCheckTask.ID, VNI: spotCheckTask.VNI, Status: 0, Timestamp: spotCheckList.Timestamp}
		_, err = collectionS.InsertOne(context.Background(), spr)
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: inserting spotcheck record of miner %d: %s", node.ID, err.Error())
			continue
		}
		node.ErrorCount = 0
		_, err = collectionSN.InsertOne(context.Background(), node)
		if err != nil {
			errstr := err.Error()
			if !strings.ContainsAny(errstr, "duplicate key error") {
				log.Errorf("ytanalysis: GetSpotCheckList: inserting miner %d to SpotCheckNode table: %s", node.ID, err.Error())
				continue
			}
		}
		return spotCheckList, nil
	}
	log.Warnf("ytanalysis: GetSpotCheckList: no nodes can be spotchecked")
	return nil, errors.New("no nodes can be spotchecked")
}

//IsNodeSelected check if node is selected for spotchecking
func (analyser *Analyser) IsNodeSelected() (bool, error) {
	collection := analyser.client.Database(DBName(analyser.SnCount, 0)).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "status": 1, "version": bson.M{"$gte": analyser.Params.MinerVersionThreshold}})
	if err != nil {
		return false, errors.New("error when get count of spotcheckable nodes")
	}
	d, err := collection.CountDocuments(context.Background(), bson.M{"status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*analyser.Params.AvaliableNodeTimeGap}, "version": bson.M{"$gte": analyser.Params.MinerVersionThreshold}})
	if err != nil {
		return false, errors.New("error when get count of spotcheck-executing nodes")
	}
	if c == 0 || d == 0 {
		return false, nil
	}
	if float32(c)/float32(d) > 2 {
		c = 2000
		d = 1000
	}
	if d < 100 {
		c = c * 100
		d = d * 100
	}
	n := rand.Int63n(d * analyser.Params.SpotCheckInterval)
	if n < c {
		return true, nil
	}
	return false, nil
}
