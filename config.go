package ytanalysis

import (
	"fmt"

	"github.com/spf13/viper"
)

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//TimeoutField Field name of timeout config
	TimeoutField = "timeout"
	//MongoDBURLField Field name of mongodb-url config
	MongoDBURLField = "mongodb-url"
	//SNCountField Field name of sn-count config
	SNCountField = "sn-count"

	//EOSURLField Field name of eos.url config
	EOSURLField = "eos.url"
	//EOSBPAccountField Field name of eos.bp-account config
	EOSBPAccountField = "eos.bp-account"
	//EOSBPPrivateKeyField Field name of eos.bp-privatekey config
	EOSBPPrivateKeyField = "eos.bp-privatekey"
	//EOSContractOwnerMField Field name of eos.contract-ownerm config
	EOSContractOwnerMField = "eos.contract-ownerm"
	//EOSContractOwnerDField Field name of eos.contract-ownerd config
	EOSContractOwnerDField = "eos.contract-ownerd"
	//EOSShadowAccountField Field name of eos.shadow-account config
	EOSShadowAccountField = "eos.shadow-account"

	//LoggerFilePathField Field name of logger.file-path config
	LoggerFilePathField = "logger.file-path"
	//LoggerRotationTimeField Field name of logger.rotation-time config
	LoggerRotationTimeField = "logger.rotation-time"
	//LoggerMaxAgeField Field name of logger.rotation-time config
	LoggerMaxAgeField = "logger.max-age"
	//LoggerLevelField Field name of logger.level config
	LoggerLevelField = "logger.level"

	//MiscRecheckingPoolLengthField Field name of misc.reckecking-pool-length config
	MiscRecheckingPoolLengthField = "misc.reckecking-pool-length"
	//MiscRecheckingQueueLengthField Field name of misc.reckecking-queue-length config
	MiscRecheckingQueueLengthField = "misc.reckecking-queue-length"
	//MiscConnectivityTestIntervalField Field name of misc.connectivity-test-interval config
	MiscConnectivityTestIntervalField = "misc.connectivity-test-interval"
	//MiscAvaliableNodeTimeGapField Field name of misc.avaliable-node-time-gap config
	MiscAvaliableNodeTimeGapField = "misc.avaliable-node-time-gap"
	//MiscMinerVersionThresholdField Field name of misc.miner-version-threshold config
	MiscMinerVersionThresholdField = "misc.miner-version-threshold"
	//MiscPunishGapUnitField Field name of misc.punish-gap-unit config
	MiscPunishGapUnitField = "misc.punish-gap-unit"
	//MiscPunishPhase1Field Field name of misc.punish-phase1 config
	MiscPunishPhase1Field = "misc.punish-phase1"
	//MiscPunishPhase1PercentField Field name of misc.punish-phase1-percent config
	MiscPunishPhase1PercentField = "misc.punish-phase1-percent"
	//MiscPunishPhase2Field Field name of misc.punish-phase2 config
	MiscPunishPhase2Field = "misc.punish-phase2"
	//MiscPunishPhase2PercentField Field name of misc.punish-phase2-percent config
	MiscPunishPhase2PercentField = "misc.punish-phase2-percent"
	//MiscPunishPhase3Field Field name of misc.punish-phase3 config
	MiscPunishPhase3Field = "misc.punish-phase3"
	//MiscPunishPhase3PercentField Field name of misc.punish-phase3-percent config
	MiscPunishPhase3PercentField = "misc.punish-phase3-percent"
	//MiscSpotCheckSkipTimeField Field name of misc.spotcheck-skip-time config
	MiscSpotCheckSkipTimeField = "misc.spotcheck-skip-time"
	//MiscSpotCheckIntervalField Field name of misc.spotcheck-interval config
	MiscSpotCheckIntervalField = "misc.spotcheck-interval"
	//MiscSpotCheckConnectTimeoutField Field name of misc.spotcheck-connect-timeout config
	MiscSpotCheckConnectTimeoutField = "misc.spotcheck-connect-timeout"
	//MiscErrorNodePercentThresholdField Field name of misc.error-node-percent-threshold config
	MiscErrorNodePercentThresholdField = "misc.error-node-percent-threshold"
	//MiscExcludeAddrPrefixField Field name of misc.exclude-addr-prefix config
	MiscExcludeAddrPrefixField = "misc.exclude-addr-prefix"
)

//Config system configuration
type Config struct {
	BindAddr   string      `mapstructure:"bind-addr"`
	Timeout    int64       `mapstructure:"timeout"`
	MongoDBURL string      `mapstructure:"mongodb-url"`
	SNCount    int64       `mapstructure:"sn-count"`
	EOS        *EOSConfig  `mapstructure:"eos"`
	Logger     *LogConfig  `mapstructure:"logger"`
	MiscConfig *MiscConfig `mapstructure:"misc"`
}

//EOSConfig EOS configuration
type EOSConfig struct {
	URL            string `mapstructure:"url"`
	BPAccount      string `mapstructure:"bp-account"`
	BPPrivateKey   string `mapstructure:"bp-privatekey"`
	ContractOwnerM string `mapstructure:"contract-ownerm"`
	ContractOwnerD string `mapstructure:"contract-ownerd"`
	ShadowAccount  string `mapstructure:"shadow-account"`
}

//LogConfig system log configuration
type LogConfig struct {
	FilePath     string `mapstructure:"file-path"`
	RotationTime int64  `mapstructure:"rotation-time"`
	MaxAge       int64  `mapstructure:"max-age"`
	Level        string `mapstructure:"level"`
}

//MiscConfig miscellaneous configuration
type MiscConfig struct {
	RecheckingPoolLength      int    `mapstructure:"reckecking-pool-length"`
	RecheckingQueueLength     int    `mapstructure:"reckecking-queue-length"`
	ConnectivityTestInterval  int32  `mapstructure:"connectivity-test-interval"`
	AvaliableNodeTimeGap      int64  `mapstructure:"avaliable-node-time-gap"`
	MinerVersionThreshold     int32  `mapstructure:"miner-version-threshold"`
	PunishGapUnit             int64  `mapstructure:"punish-gap-unit"`
	PunishPhase1              int32  `mapstructure:"punish-phase1"`
	PunishPhase1Percent       int32  `mapstructure:"punish-phase1-percent"`
	PunishPhase2              int32  `mapstructure:"punish-phase2"`
	PunishPhase2Percent       int32  `mapstructure:"punish-phase2-percent"`
	PunishPhase3              int32  `mapstructure:"punish-phase3"`
	PunishPhase3Percent       int32  `mapstructure:"punish-phase3-percent"`
	SpotCheckSkipTime         int64  `mapstructure:"spotcheck-skip-time"`
	SpotCheckInterval         int64  `mapstructure:"spotcheck-interval"`
	SpotCheckConnectTimeout   int64  `mapstructure:"spotcheck-connect-timeout"`
	ErrorNodePercentThreshold int32  `mapstructure:"error-node-percent-threshold"`
	ExcludeAddrPrefix         string `mapstructure:"exclude-addr-prefix"`
}

//InitializeConfig initialize config structure
func InitializeConfig() *Config {
	config := new(Config)
	viper.SetDefault(BindAddrField, ":8080")
	viper.SetDefault(TimeoutField, 6000)
	viper.SetDefault(MongoDBURLField, "mongodb://127.0.0.1:27017/?connect=direct")
	viper.SetDefault(SNCountField, 5)
	viper.SetDefault(EOSURLField, "http://127.0.0.1:8888")
	viper.SetDefault(EOSBPAccountField, "testaccount1")
	viper.SetDefault(EOSBPPrivateKeyField, "00000000000")
	viper.SetDefault(EOSContractOwnerMField, "testaccount1")
	viper.SetDefault(EOSContractOwnerDField, "testaccount1")
	viper.SetDefault(EOSShadowAccountField, "testaccount1")
	viper.SetDefault(LoggerFilePathField, "./spotcheck.log")
	viper.SetDefault(LoggerRotationTimeField, 24)
	viper.SetDefault(LoggerMaxAgeField, 2400)
	viper.SetDefault(LoggerLevelField, "Debug")
	viper.SetDefault(MiscRecheckingPoolLengthField, 5000)
	viper.SetDefault(MiscRecheckingQueueLengthField, 10000)
	viper.SetDefault(MiscConnectivityTestIntervalField, 60)
	viper.SetDefault(MiscAvaliableNodeTimeGapField, 3)
	viper.SetDefault(MiscMinerVersionThresholdField, 0)
	viper.SetDefault(MiscPunishGapUnitField, 60)
	viper.SetDefault(MiscPunishPhase1Field, 1440)
	viper.SetDefault(MiscPunishPhase2Field, 4320)
	viper.SetDefault(MiscPunishPhase3Field, 10080)
	viper.SetDefault(MiscPunishPhase1PercentField, 1)
	viper.SetDefault(MiscPunishPhase2PercentField, 10)
	viper.SetDefault(MiscPunishPhase3PercentField, 50)
	viper.SetDefault(MiscSpotCheckSkipTimeField, 0)
	viper.SetDefault(MiscSpotCheckIntervalField, 60)
	viper.SetDefault(MiscSpotCheckConnectTimeoutField, 10)
	viper.SetDefault(MiscErrorNodePercentThresholdField, 95)
	viper.SetDefault(MiscExcludeAddrPrefixField, "")
	if err := viper.Unmarshal(config); err != nil {
		panic(fmt.Sprintf("unable to decode into struct, %v\n", err))
	}
	return config
}
