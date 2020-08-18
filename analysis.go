package ytanalysis

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

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

//Analyser analyser
type Analyser struct {
	//clients          []*mongo.Client
	analysisdbClient *mongo.Client
	//dbnameIndexed    bool
	// eostx   *eostx.EosTX
	checker *Host
	//SnCount          int64
	Params *MiscConfig
	pool   *grpool.Pool
	mqClis map[int]*ytsync.Service
}

//New create new analyser instance
func New(analysisDBURL string, mqconf *AuraMQConfig, conf *MiscConfig) (*Analyser, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	analysisdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	entry.Infof("created analysisDB client: %s", analysisDBURL)
	// etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount)
	// if err != nil {
	// 	entry.WithError(err).Errorf("creating EOS client failed: %s", eosURL)
	// 	return nil, err
	// }
	// entry.Infof("created EOS client: %s", eosURL)
	host, err := NewHost()
	if err != nil {
		entry.WithError(err).Error("creating new host failed")
		return nil, err
	}
	entry.Info("creating host successful")
	pool := grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)
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
					syncNode(analysisdbClient, node, conf.ExcludeAddrPrefix)
				}
			}
		}
	}
	m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	if err != nil {
		entry.WithError(err).Error("creating mq clients map failed")
		return nil, err
	}
	analyser := &Analyser{analysisdbClient: analysisdbClient, checker: host, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength), mqClis: m}
	return analyser, nil
}

func syncNode(cli *mongo.Client, node *Node, excludeAddrPrefix string) error {
	entry := log.WithFields(log.Fields{Function: "SyncNode"})
	if node.ID == 0 {
		return errors.New("node ID cannot be 0")
	}
	node.Addrs = checkPublicAddrs(node.Addrs, excludeAddrPrefix)
	collection := cli.Database(AnalysisDB).Collection(NodeTab)
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

	cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
	for k, v := range node.Uspaces {
		cond[fmt.Sprintf("uspaces.%s", k)] = v
	}
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.Before)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
	oldNode := new(Node)
	err := result.Decode(oldNode)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			_, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc})
			if err != nil {
				entry.WithError(err).Warnf("inserting node %d to database", node.ID)
				return err
			}
			return nil
		}
		entry.WithError(err).Warnf("updating record of node %d", node.ID)
		return err
	}
	if oldNode.Status != node.Status {
		collectionSN := cli.Database(AnalysisDB).Collection(SpotCheckNodeTab)
		_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": node.Status}})
		if err != nil {
			entry.WithError(err).Warnf("updating status of spotcheck node %d", node.ID)
			return err
		}
	}
	return nil

	// _, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc})
	// if err != nil {
	// 	errstr := err.Error()
	// 	if !strings.ContainsAny(errstr, "duplicate key error") {
	// 		entry.WithError(err).Warnf("inserting node %d to database", node.ID)
	// 		return err
	// 	}
	// 	oldNode := new(Node)
	// 	err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(oldNode)
	// 	if err != nil {
	// 		entry.WithError(err).Warnf("fetching node %d failed", node.ID)
	// 		return err
	// 	}
	// 	cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
	// 	for k, v := range node.Uspaces {
	// 		cond[fmt.Sprintf("uspaces.%s", k)] = v
	// 	}
	// 	opts := new(options.FindOneAndUpdateOptions)
	// 	opts = opts.SetReturnDocument(options.After)
	// 	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
	// 	updatedNode := new(Node)
	// 	err = result.Decode(updatedNode)
	// 	if err != nil {
	// 		entry.WithError(err).Warnf("updating record of node %d", node.ID)
	// 		return err
	// 	}
	// 	if oldNode.Status != updatedNode.Status {
	// 		collectionSN := cli.Database(AnalysisDB).Collection(SpotCheckNodeTab)
	// 		_, err := collectionSN.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": updatedNode.Status}})
	// 		if err != nil {
	// 			entry.WithError(err).Warnf("updating status of spotcheck node %d", node.ID)
	// 			return err
	// 		}
	// 	}
	// }
	// return nil
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
