package ytanalysis

import (
	"context"

	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Analyser analyser
type Analyser struct {
	//clients          []*mongo.Client
	nodeMgr          *NodeManager
	analysisdbClient *mongo.Client
	//dbnameIndexed    bool
	// eostx   *eostx.EosTX
	checker *Host
	//SnCount          int64
	Params *MiscConfig
	pool   *grpool.Pool
	//mqClis map[int]*ytsync.Service
	// c      int64
	// d      int64
}

//New create new analyser instance
func New(ctx context.Context, analysisDBURL, syncDBURL string, mqconf *AuraMQConfig, conf *MiscConfig) (*Analyser, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	analysisdbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	syncdbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(syncDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating syncDB client failed: %s", syncDBURL)
		return nil, err
	}
	entry.Infof("created syncDB client: %s", analysisDBURL)
	taskManager := NewTaskManager(analysisdbClient, syncdbClient, conf.SpotCheckStartTime, conf.SpotCheckEndTime)
	nodeManager, err := NewNodeManager(ctx, analysisdbClient, taskManager, mqconf, conf.RecheckingPoolLength, conf.RecheckingQueueLength, conf.MinerVersionThreshold, conf.AvaliableNodeTimeGap, conf.SpotCheckInterval, conf.ExcludeAddrPrefix)
	if err != nil {
		entry.WithError(err).Error("creating node manager failed")
		return nil, err
	}
	entry.Info("created node manager")
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
	// pool := grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)
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
	// 				syncNode(analysisdbClient, node, conf.ExcludeAddrPrefix)
	// 			}
	// 		}
	// 	}
	// }
	// m, err := ytsync.StartSync(mqconf.SubscriberBufferSize, mqconf.PingWait, mqconf.ReadWait, mqconf.WriteWait, mqconf.MinerSyncTopic, mqconf.AllSNURLs, callback, mqconf.Account, mqconf.PrivateKey, mqconf.ClientID)
	// if err != nil {
	// 	entry.WithError(err).Error("creating mq clients map failed")
	// 	return nil, err
	// }
	analyser := &Analyser{nodeMgr: nodeManager, analysisdbClient: analysisdbClient, checker: host, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)}
	// err = analyser.calculateCD()
	// if err != nil {
	// 	entry.WithError(err).Error("calculate c & d failed")
	// 	return nil, err
	// }
	return analyser, nil
}
