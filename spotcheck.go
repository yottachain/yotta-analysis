package ytanalysis

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/eoscanada/eos-go"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
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
	log.Infof("ytanalysis: StartRecheck: starting recheck executor...\n")
	log.Infof("ytanalysis: StartRecheck: clear spotcheck tasks...\n")
	collection := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	_, err := collection.DeleteMany(context.Background(), bson.M{"status": 0})
	if err != nil {
		log.Warnf("ytanalysis: StartRecheck: deleting spotcheck tasks with status 0: %s\n", err.Error())
	}
	_, err = collection.UpdateMany(context.Background(), bson.M{"status": 2}, bson.M{"$set": bson.M{"status": 1}})
	if err != nil {
		log.Warnf("ytanalysis: StartRecheck: updatING spotcheck tasks with status 2 to 1: %s\n", err.Error())
	}
	go analyser.doRecheck()
}

func (analyser *Analyser) selectNodeTab(minerID int32) *mongo.Collection {
	return analyser.client.Database(DBName(analyser.SnCount, minerID)).Collection(NodeTab)
}

func (analyser *Analyser) doRecheck() {
	pool := grpool.NewPool(5000, 10000)
	defer pool.Release()
	collection := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionErr := analyser.client.Database(AnalysisDB).Collection(ErrorNodeTab)
	for {
		cur, err := collection.Find(context.Background(), bson.M{"status": 1})
		if err != nil {
			log.Warnf("ytanalysis: doRecheck: selecting recheckable task: %s\n", err.Error())
			time.Sleep(time.Second * time.Duration(10))
			continue
		}
		for cur.Next(context.Background()) {
			var flag = true
			spr := new(SpotCheckRecord)
			err := cur.Decode(spr)
			if err != nil {
				log.Warnf("ytanalysis: doRecheck: decoding recheckable task: %s\n", err.Error())
				continue
			}
			n := new(Node)
			err = analyser.selectNodeTab(spr.NID).FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(n)
			if err != nil {
				log.Warnf("ytanalysis: doRecheck: decoding node: %s\n", err.Error())
				_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Warnf("ytanalysis: doRecheck: deleting task related a non-exist node: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				}
				continue
			}
			if n.Status > 1 {
				_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Warnf("ytanalysis: doRecheck: deleting task with node status bigger than 1: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				}
				continue
			}

			t := time.Now().Unix() - spr.Timestamp
			for i := range [10080]byte{} {
				if t > analyser.Params.PunishGapUnit*int64(i+1) && spr.ErrCount == int64(i) {
					_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"errCount": i + 1}})
					if err != nil {
						log.Errorf("ytanalysis: doRecheck: updating error count: %s\n", err.Error())
						break
					}
					spr.ErrCount = int64(i + 1)
					// if i+1 == int(rebuildPhase) {
					// 	_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
					// 	if err != nil {
					// 		log.Printf("spotcheck: doRecheck: error when updating rebuild status: %s\n", err.Error())
					// 	}
					// 	err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
					// 	if err != nil {
					// 		log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
					// 	}
					// 	flag = false
					// }

					if i+1 == int(analyser.Params.PunishPhase1) {
						log.Infof("ytanalysis: doRecheck: miner %d has been offline for %d seconds, do phase1 punishment\n", spr.NID, analyser.Params.PunishGapUnit*int64(i+1))
						if analyser.Params.PunishPhase1Percent > 0 {
							if analyser.ifNeedPunish(spr.NID, n.PoolOwner) {
								left, err := analyser.punish(n, int64(analyser.Params.PunishPhase1Percent))
								if err != nil {
									log.Errorf("ytanalysis: doRecheck: punishing %d%%: %s\n", analyser.Params.PunishPhase1Percent, err.Error())
								} else if flag && left == 0 {
									log.Infof("ytanalysis: doRecheck: no deposit can be punished: %d\n", n.ID)
									spr.Status = 3
									_, err := collectionErr.InsertOne(context.Background(), spr)
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										break
									}
									_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: deleting deposit exhausted task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
										break
									}
									//TODO: how to modify node
									_, err = analyser.selectNodeTab(spr.NID).UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: updating rebuild status: %s\n", err.Error())
									}
									// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
									// if err != nil {
									// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
									// }
									flag = false
								}
							}
						}
						break
					}
					if i+1 == int(analyser.Params.PunishPhase2) {
						log.Infof("ytanalysis: doRecheck: miner %d has been offline for %d seconds, do phase2 punishment\n", spr.NID, analyser.Params.PunishGapUnit*int64(i+1))
						if analyser.Params.PunishPhase2Percent > 0 {
							if analyser.ifNeedPunish(spr.NID, n.PoolOwner) {
								left, err := analyser.punish(n, int64(analyser.Params.PunishPhase2Percent))
								if err != nil {
									log.Errorf("ytanalysis: doRecheck: error when punishing %d%%: %s\n", analyser.Params.PunishPhase2Percent, err.Error())
								} else if flag && left == 0 {
									log.Infof("spotcheck: doRecheck: no deposit can be punished: %d\n", n.ID)
									spr.Status = 3
									_, err := collectionErr.InsertOne(context.Background(), spr)
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										break
									}
									_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: deleting deposit exhausted task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
										break
									}
									//TODO: how to modify node
									_, err = analyser.selectNodeTab(spr.NID).UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
									if err != nil {
										log.Errorf("ytanalysis: doRecheck: updating exhausted Node %d: %s\n", n.ID, err.Error())
									}
									// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
									// if err != nil {
									// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
									// }
									flag = false
								}
							}
						}
						break
					}
					if i+1 == int(analyser.Params.PunishPhase3) {
						log.Infof("ytanalysis: doRecheck: miner %d has been offline for %d seconds, do phase3 punishment\n", spr.NID, analyser.Params.PunishGapUnit*int64(i+1))
						spr.Status = 1
						_, err := collectionErr.InsertOne(context.Background(), spr)
						if err != nil {
							log.Errorf("ytanalysis: doRecheck: inserting timeout task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
							break
						}
						_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
						if err != nil {
							log.Errorf("ytanalysis: doRecheck: deleting timeout task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
							collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
							break
						}
						//TODO: how to modify node
						_, err = analyser.selectNodeTab(spr.NID).UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
						if err != nil {
							log.Errorf("ytanalysis: doRecheck: updating rebuild status: %s\n", err.Error())
						}
						// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
						// if err != nil {
						// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
						// }
						if analyser.Params.PunishPhase3Percent > 0 {
							if analyser.ifNeedPunish(spr.NID, n.PoolOwner) {
								_, err = analyser.punish(n, int64(analyser.Params.PunishPhase3Percent))
								if err != nil {
									log.Errorf("ytanalysis: doRecheck: doing phase3 punishment: %s\n", err.Error())
								}
							}
						}
						flag = false
					}
				}
			}

			if flag {
				//calculate probability for rechecking
				r := rand.Int63n(10 * int64(analyser.Params.SpotCheckInterval))
				if r < 10 {
					// rechecking
					_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
					if err != nil {
						log.Errorf("ytanalysis: doRecheck: updating recheckable task to status 2: %s\n", err.Error())
						cur.Close(context.Background())
						continue
					}
					spr.Status = 2
					pool.JobQueue <- func() {
						analyser.checkDataNode(spr)
					}
				}
			}
		}
		cur.Close(context.Background())
		time.Sleep(time.Second * time.Duration(10))
	}
}

func (analyser *Analyser) ifNeedPunish(minerID int32, poolOwner string) bool {
	collection := analyser.client.Database(DBName(analyser.SnCount, minerID)).Collection(PoolWeightTab)
	result := new(PoolWeight)
	err := collection.FindOne(context.Background(), bson.M{"_id": poolOwner}).Decode(result)
	if err != nil {
		log.Errorf("ytanalysis: ifNeedPunish: getting pool weight of %s: %s\n", poolOwner, err.Error())
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
	log.Infof("ytanalysis: checkDataNode: SN rechecking task: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
	//collection := self.client.Database(YottaDB).Collection(NodeTab)
	collectionSpotCheck := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	collectionErr := analyser.client.Database(AnalysisDB).Collection(ErrorNodeTab)
	n := new(Node)
	err := analyser.selectNodeTab(spr.NID).FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(n)
	if err != nil {
		log.Errorf("ytanalysis: checkDataNode: decoding node which performing rechecking: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
		_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Errorf("ytanalysis: checkDataNode: updating task %s status to 1: %s\n", spr.TaskID, err.Error())
		}
		return
	}
	if n.Status > 1 {
		_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Errorf("ytanalysis: checkDataNode: updating task %s status to 1 whose node status is bigger than 1: %s\n", spr.TaskID, err.Error())
		}
		return
	}
	b, err := analyser.CheckVNI(n, spr)
	if err != nil {
		log.Warnf("ytanalysis: checkDataNode: vni check error: %d -> %s -> %s: %s\n", spr.NID, spr.TaskID, spr.VNI, err.Error())
		_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Errorf("ytanalysis: checkDataNode: updating task %s status to 1: %s\n", spr.TaskID, err.Error())
		}
	} else {
		if b {
			_, err := collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: deleting task %s: %s\n", spr.TaskID, err.Error())
			} else {
				log.Infof("spotcheck: checkDataNode: vni check success: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
			}
		} else {
			errstr := ""
			if err != nil {
				errstr = err.Error()
			}
			log.Infof("ytanalysis: checkDataNode: vni check failed, checking 100 more VNIs: %d -> %s -> %s: %s\n", spr.NID, spr.TaskID, spr.VNI, errstr)
			i := 0
			var errCount int64 = 0
			for range [100]byte{} {
				i++
				spr.VNI, err = analyser.GetRandomVNI(n.ID)
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: get random vni%d error: %d -> %s -> %s: %s\n", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Errorf("ytanalysis: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
					}
					return
				}
				b, err := analyser.CheckVNI(n, spr)
				if err != nil {
					log.Warnf("ytanalysis: checkDataNode: vni%d check error: %d -> %s -> %s: %s\n", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Errorf("ytanalysis: checkDataNode: updating task %s status to 1: %s\n", spr.TaskID, err.Error())
					}
					return
				}
				if !b {
					errCount++
				}
			}
			spr.ErrCount = errCount
			spr.Status = 2
			log.Infof("ytanalysis: checkDataNode: finished 100 VNIs check, %d verify errors in %d checks\n", errCount, i)
			if errCount == 100 {
				_, err := collectionErr.InsertOne(context.Background(), spr)
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: inserting verified error task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
					return
				}
			}
			_, err = collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Errorf("ytanalysis: checkDataNode: deleting verify error task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				_, err := collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: deleting spotcheck task %s: %s\n", spr.TaskID, err.Error())
				}
				_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: update spotcheck task %s status to 1: %s\n", spr.TaskID, err.Error())
				}
				return
			}
			if errCount == 100 {
				log.Warnf("ytanalysis: checkDataNode: 100/100 random VNI checking of miner %d has failed, punish %d%% deposit\n", spr.NID, analyser.Params.PunishPhase3Percent)
				if analyser.Params.PunishPhase3Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, n.PoolOwner) {
						_, err := analyser.punish(n, int64(analyser.Params.PunishPhase3Percent))
						if err != nil {
							log.Errorf("ytanalysis: checkDataNode: punishing node %d 50%% deposit: %s\n", spr.NID, err.Error())
						}
					}
				}
				// err = self.eostx.CalculateProfit(n.ProfitAcc, uint64(n.ID), false)
				// if err != nil {
				// 	log.Printf("spotcheck: checkDataNode: error when stopping profit calculation: %s\n", err.Error())
				// }
				//TODO: how to modify node
				_, err = analyser.selectNodeTab(spr.NID).UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
				if err != nil {
					log.Errorf("ytanalysis: checkDataNode: updating rebuild status: %s\n", err.Error())
				}
			} else {
				log.Infof("ytanalysis: checkDataNode: %d/100 random VNI checking of miner %d has failed, punish %d%% deposit\n", errCount, spr.NID, analyser.Params.PunishPhase1Percent)
				if analyser.Params.PunishPhase1Percent > 0 {
					if analyser.ifNeedPunish(spr.NID, n.PoolOwner) {
						left, err := analyser.punish(n, int64(analyser.Params.PunishPhase1Percent))
						if err != nil {
							log.Errorf("ytanalysis: checkDataNode: punishing node %d 1%% deposit: %s\n", spr.NID, err.Error())
						} else if left == 0 {
							log.Warnf("ytanalysis: checkDataNode: no deposit can be punished: %d\n", spr.NID)
							spr.Status = 3
							_, err := collectionErr.InsertOne(context.Background(), spr)
							if err != nil {
								log.Errorf("ytanalysis: checkDataNode: inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
								return
							}
							// err = self.eostx.CalculateProfit(n.ProfitAcc, uint64(n.ID), false)
							// if err != nil {
							// 	log.Printf("spotcheck: checkDataNode: error when stopping profit calculation: %s\n", err.Error())
							// }
							//TODO: how to modify node
							_, err = analyser.selectNodeTab(spr.NID).UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
							if err != nil {
								log.Errorf("ytanalysis: checkDataNode: updating rebuild status: %s\n", err.Error())
							}
							_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 3}})
							if err != nil {
								log.Errorf("ytanalysis: checkDataNode: updating error node status to %d: %s\n", 3, err.Error())
							}
						}
					}
				}
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
		log.Errorf("ytanalysis: CheckVNI: unmarshaling VNI: %d %s %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	rawvni = rawvni[len(rawvni)-16:]
	downloadRequest := &pb.DownloadShardRequest{VHF: rawvni}
	checkData, err := proto.Marshal(downloadRequest)
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: marshalling protobuf message: downloadrequest: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	shardData, err := analyser.checker.SendMsg(node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: SN send recheck command failed: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	var share pb.DownloadShardResponse
	err = proto.Unmarshal(shardData[2:], &share)
	if err != nil {
		log.Errorf("ytanalysis: CheckVNI: SN unmarshal recheck response failed: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
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
	log.Infof("ytanalysis: Punish: punish %d%% deposit of node %d: %d\n", percent, node.ID, punishFee)
	return retLeft, nil
}

//UpdateTaskStatus process error task of spotchecking
func (analyser *Analyser) UpdateTaskStatus(taskid string, invalidNodeList []int32) error {
	collection := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	for _, id := range invalidNodeList {
		spr := new(SpotCheckRecord)
		err := collection.FindOne(context.Background(), bson.M{"_id": taskid}).Decode(spr)
		if err != nil {
			log.Errorf("ytanalysis: UpdateTaskStatus: fetching spotcheck task in mongodb: %d -> %s -> %s\n", id, taskid, err.Error())
			continue
		}
		if spr.Status > 0 {
			log.Warnf("ytanalysis: UpdateTaskStatus: task is already under rechecking: %d -> %s -> %s\n", id, taskid, err.Error())
			continue
		}
		sping := new(SpotCheckRecord)
		err = collection.FindOne(context.Background(), bson.M{"nid": id, "status": bson.M{"$gt": 0}}).Decode(sping)
		if err != nil {
			log.Errorf("ytanalysis: UpdateTaskStatus: update task %s status to 1: %s\n", taskid, err.Error())
			_, err := collection.UpdateOne(context.Background(), bson.M{"_id": taskid}, bson.M{"$set": bson.M{"status": 1, "timestamp": time.Now().Unix()}})
			if err != nil {
				log.Errorf("ytanalysis: UpdateTaskStatus: updating task %s status to 1: %s\n", spr.TaskID, err.Error())
			}
		}
		_, err = collection.DeleteMany(context.Background(), bson.M{"nid": id, "status": 0})
		if err != nil {
			log.Errorf("ytanalysis: UpdateTaskStatus: deleting tasks with node ID %d: %s\n", id, err.Error())
		}
	}
	return nil
}

//GetRandomVNI find one VNI by miner ID and index of DNI table
func (analyser *Analyser) GetRandomVNI(id int32) (string, error) {
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
	spotCheckList := new(SpotCheckList)
	snID := rand.Int31n(int32(analyser.SnCount))
	collection := analyser.client.Database(DBName(analyser.SnCount, snID)).Collection(NodeTab)
	collectionSpotCheck := analyser.client.Database(AnalysisDB).Collection(SpotCheckTab)
	for range [10]byte{} {
		total, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1})
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: calculating total count of spotcheckable nodes: %s\n", err.Error())
			continue
		}
		if total == 0 {
			break
		}
		n := rand.Intn(int(total))
		optionf := new(options.FindOptions)
		skip := int64(n)
		limit := int64(1)
		optionf.Limit = &limit
		optionf.Skip = &skip
		cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{analyser.SnCount, snID}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1}, optionf)
		if err != nil {
			log.Errorf("ytanalysis: GetSpotCheckList: fetching spotcheckable node: %s\n", err.Error())
			continue
		}
		if cur.Next(context.Background()) {
			node := new(Node)
			err := cur.Decode(node)
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: decoding spotcheck task: %s\n", err.Error())
				cur.Close(context.Background())
				continue
			}
			log.Infof("ytanalysis: GetSpotCheckList: node %d wil be spotchecked\n", node.ID)
			spotCheckTask := new(SpotCheckTask)
			spotCheckTask.ID = node.ID
			spotCheckTask.NodeID = node.NodeID
			addr := GetRelayURL(node.Addrs)
			if addr != "" {
				spotCheckTask.Addr = addr
			} else {
				spotCheckTask.Addr = analyser.CheckPublicAddr(node.Addrs)
			}
			spotCheckTask.VNI, err = analyser.GetRandomVNI(node.ID)
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: selecting random vni: %d %s\n", node.ID, err.Error())
				cur.Close(context.Background())
				continue
			}
			log.Infof("ytanalysis: GetSpotCheckList: select random VNI for node %d -> %s\n", node.ID, spotCheckTask.VNI)
			spotCheckList.TaskID = primitive.NewObjectID()
			spotCheckList.Timestamp = time.Now().Unix()
			spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)
			cur.Close(context.Background())

			sping := new(SpotCheckRecord)
			err = collectionSpotCheck.FindOne(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": bson.M{"$gt": 0}}).Decode(sping)
			if err == nil {
				log.Warnf("ytanalysis: GetSpotCheckList: node %d is already under rechecking: %s\n", spotCheckTask.ID, sping.TaskID)
				_, err = collectionSpotCheck.DeleteMany(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": 0})
				if err != nil {
					log.Errorf("ytanalysis: GetSpotCheckList: deleting spotcheck task when another task with same node is under rechecking: %d %s\n", node.ID, err.Error())
				}
				return nil, fmt.Errorf("a spotcheck task with node id %d is under rechecking: %s", spotCheckTask.ID, spotCheckList.TaskID)
			}

			spr := &SpotCheckRecord{TaskID: spotCheckList.TaskID.Hex(), NID: spotCheckTask.ID, VNI: spotCheckTask.VNI, Status: 0, Timestamp: spotCheckList.Timestamp}
			_, err = collectionSpotCheck.InsertOne(context.Background(), spr)
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: getting spotcheck vni: %d %s\n", node.ID, err.Error())
				continue
			}

			//delete expired task
			_, err = collectionSpotCheck.DeleteMany(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": 0, "timestamp": bson.M{"$lt": time.Now().Unix() - 3600*24}})
			if err != nil {
				log.Errorf("ytanalysis: GetSpotCheckList: error when deleting timeout spotcheck task of node: %d %s\n", node.ID, err.Error())
			}

			return spotCheckList, nil
		}
		cur.Close(context.Background())
		continue
	}
	log.Infof("ytanalysis: GetSpotCheckList: no nodes can be spotchecked\n")
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
