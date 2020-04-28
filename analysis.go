package ytanalysis

import (
	"context"

	"github.com/ivpusic/grpool"
	log "github.com/sirupsen/logrus"
	"github.com/yottachain/yotta-analysis/eostx"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

//Analyser analyse
type Analyser struct {
	client  *mongo.Client
	eostx   *eostx.EosTX
	checker *Host
	SnCount int64
	Params  *MiscConfig
	pool    *grpool.Pool
}

//New create new analyser instance
func New(mongoURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount string, snCount int64, conf *MiscConfig) (*Analyser, error) {
	entry := log.WithFields(log.Fields{Function: "New"})
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		entry.WithError(err).Errorf("creating mongoDB client failed: %s", mongoURL)
		return nil, err
	}
	entry.Infof("created mongoDB client: %s", mongoURL)
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
	analyser := &Analyser{client: client, eostx: etx, checker: host, SnCount: snCount, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)}
	return analyser, nil
}
