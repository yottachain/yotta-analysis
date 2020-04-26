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
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Errorf("ytanalysis: New: creating mongodb client failed: %s %s", mongoURL, err.Error())
		return nil, err
	}
	log.Infof("ytanalysis: New: create mongodb client: %s", mongoURL)
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount)
	if err != nil {
		log.Errorf("ytanalysis: New: creating eos client failed: %s %s", eosURL, err.Error())
		return nil, err
	}
	log.Infof("ytanalysis: New: create eos client: %s", eosURL)
	host, err := NewHost()
	if err != nil {
		log.Errorf("ytanalysis: New: creating new host failed: %s", err.Error())
		return nil, err
	}
	log.Info("ytanalysis: New: creating host successful")
	analyser := &Analyser{client: client, eostx: etx, checker: host, SnCount: snCount, Params: conf, pool: grpool.NewPool(conf.RecheckingPoolLength, conf.RecheckingQueueLength)}
	return analyser, nil
}
