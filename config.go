package ytanalysis

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//AnalysisDBURLField Field name of analysisdb-url config
	AnalysisDBURLField = "analysisdb-url"
	//MongoDBURLSField Field name of mongodb-urls config
	MongoDBURLSField = "mongodb-urls"
	//DBNameIndexedField Field name of dbname-indexed config
	DBNameIndexedField = "dbname-indexed"
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

	//LoggerOutputField Field name of logger.output config
	LoggerOutputField = "logger.output"
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
	//MiscAvaliableNodeTimeGapField Field name of misc.avaliable-node-time-gap config
	MiscAvaliableNodeTimeGapField = "misc.avaliable-node-time-gap"
	//MiscMinerVersionThresholdField Field name of misc.miner-version-threshold config
	MiscMinerVersionThresholdField = "misc.miner-version-threshold"
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
	BindAddr      string      `mapstructure:"bind-addr"`
	AnalysisDBURL string      `mapstructure:"analysisdb-url"`
	MongoDBURLs   []string    `mapstructure:"mongodb-urls"`
	DBNameIndexed bool        `mapstructure:"dbname-indexed"`
	SNCount       int64       `mapstructure:"sn-count"`
	EOS           *EOSConfig  `mapstructure:"eos"`
	Logger        *LogConfig  `mapstructure:"logger"`
	MiscConfig    *MiscConfig `mapstructure:"misc"`
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
	Output       string `mapstructure:"output"`
	FilePath     string `mapstructure:"file-path"`
	RotationTime int64  `mapstructure:"rotation-time"`
	MaxAge       int64  `mapstructure:"max-age"`
	Level        string `mapstructure:"level"`
}

//MiscConfig miscellaneous configuration
type MiscConfig struct {
	RecheckingPoolLength      int    `mapstructure:"reckecking-pool-length"`
	RecheckingQueueLength     int    `mapstructure:"reckecking-queue-length"`
	AvaliableNodeTimeGap      int64  `mapstructure:"avaliable-node-time-gap"`
	MinerVersionThreshold     int32  `mapstructure:"miner-version-threshold"`
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
