package ytanalysis

import (
	"context"
	"fmt"

	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/yotta-analysis/eostx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Analyser analyse
type Analyser struct {
	clients          []*mongo.Client
	analysisdbClient *mongo.Client
	dbnameIndexed    bool
	eostx            *eostx.EosTX
	checker          *Host
	SnCount          int64
	Params           *MiscConfig
	pool             *grpool.Pool
}

//New create new analyser instance
func New(mongoURLs []string, dbnameIndexed bool, analysisDBURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount string, snCount int64, conf *MiscConfig) (*Analyser, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	clients := make([]*mongo.Client, 0)
	for i, mongoURL := range mongoURLs {
		client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
		if err != nil {
			entry.WithError(err).Errorf("creating mongoDB client%d failed: %s", i, mongoURL)
			return nil, err
		}
		entry.Infof("created mongoDB client%d: %s", i, mongoURL)
		clients = append(clients, client)
	}
	analysisdbClient, err := mongo.Connect(context.Background(), options.Client().ApplyURI(analysisDBURL))
	if err != nil {
		entry.WithError(err).Errorf("creating analysisDB client failed: %s", analysisDBURL)
		return nil, err
	}
	entry.Infof("created analysisDB client: %s", analysisDBURL)
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount)
	if err != nil {
		entry.WithError(err).Errorf("creating EOS client failed: %s", eosURL)
		return nil, err
	}
	entry.Infof("created EOS client: %s", eosURL)
	host, err := NewHost()
	if err != nil {
		entry.WithError(err).Error("creating new host failed")
		return nil, err
	}
	entry.Info("creating host successful")
	analyser := &Analyser{clients: clients, analysisdbClient: analysisdbClient, dbnameIndexed: dbnameIndexed, eostx: etx, checker: host, SnCount: snCount, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)}
	return analyser, nil
}

//SelectYottaDB select mongoDB client by miner ID
func (analyser *Analyser) SelectYottaDB(minerID int32) *mongo.Database {
	snID := int64(minerID) % analyser.SnCount
	dbname := YottaDB
	if analyser.dbnameIndexed {
		dbname = fmt.Sprintf("%s%d", dbname, snID)
	}
	return analyser.clients[snID].Database(dbname)
}
