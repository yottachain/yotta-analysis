package ytanalysis

import (
	"context"
	"net/http"

	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
)

//Analyser analyser
type Analyser struct {
	nodeMgr          *NodeManager
	analysisdbClient *mongo.Client
	httpCli          *http.Client
	minerStat        *MinerStatConfig
	checker          *Host
	Params           *MiscConfig
	pool             *grpool.Pool
}

//New create new analyser instance
func New(ctx context.Context, analysisDBURL string, pdURLs []string, mqconf *AuraMQConfig, msConfig *MinerStatConfig, conf *MiscConfig) (*Analyser, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	analysisdbClient, err := mongo.Connect(ctx, options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	tikvCli, err := rawkv.NewClient(ctx, pdURLs, config.Default())
	if err != nil {
		entry.WithError(err).Errorf("creating tikv client failed: %v", pdURLs)
		return nil, err
	}

	entry.Infof("created syncDB client: %s", analysisDBURL)
	taskManager := NewTaskManager(tikvCli, conf.SpotCheckStartTime, conf.SpotCheckEndTime)
	nodeManager, err := NewNodeManager(ctx, analysisdbClient, taskManager, mqconf, conf.RecheckingPoolLength, conf.RecheckingQueueLength, conf.MinerVersionThreshold, conf.AvaliableNodeTimeGap, conf.SpotCheckInterval, conf.ExcludeAddrPrefix)
	if err != nil {
		entry.WithError(err).Error("creating node manager failed")
		return nil, err
	}
	entry.Info("created node manager")
	host, err := NewHost()
	if err != nil {
		entry.WithError(err).Error("creating new host failed")
		return nil, err
	}
	entry.Info("creating host successful")
	analyser := &Analyser{nodeMgr: nodeManager, analysisdbClient: analysisdbClient, httpCli: &http.Client{}, minerStat: msConfig, checker: host, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)}
	return analyser, nil
}
