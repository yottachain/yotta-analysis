package ytanalysis

const (
	//BindAddrField Field name of bind-addr config
	BindAddrField = "bind-addr"
	//AnalysisDBURLField Field name of analysisdb-url config
	AnalysisDBURLField = "analysisdb-url"

	//AuramqSubscriberBufferSizeField Field name of auramq.subscriber-buffer-size
	AuramqSubscriberBufferSizeField = "auramq.subscriber-buffer-size"
	//AuramqPingWaitField Field name of auramq.ping-wait
	AuramqPingWaitField = "auramq.ping-wait"
	//AuramqReadWaitField Field name of auramq.read-wait
	AuramqReadWaitField = "auramq.read-wait"
	//AuramqWriteWaitField Field name of auramq.write-wait
	AuramqWriteWaitField = "auramq.write-wait"
	//AuramqMinerSyncTopicField Field name of auramq.miner-sync-topic
	AuramqMinerSyncTopicField = "auramq.miner-sync-topic"
	//AuramqAllSNURLsField Field name of auramq.all-sn-urls
	AuramqAllSNURLsField = "auramq.all-sn-urls"
	//AuramqAccountField Field name of auramq.account
	AuramqAccountField = "auramq.account"
	//AuramqPrivateKeyField Field name of auramq.private-key
	AuramqPrivateKeyField = "auramq.private-key"

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
	//MiscPoolErrorMinerTimeThresholdField Field name of misc.pool-error-miner-time-threshold
	MiscPoolErrorMinerTimeThresholdField = "pool-error-miner-time-threshold"
	//MiscExcludeAddrPrefixField Field name of misc.exclude-addr-prefix config
	MiscExcludeAddrPrefixField = "misc.exclude-addr-prefix"
)

//Config system configuration
type Config struct {
	BindAddr      string        `mapstructure:"bind-addr"`
	AnalysisDBURL string        `mapstructure:"analysisdb-url"`
	AuraMQ        *AuraMQConfig `mapstructure:"auramq"`
	Logger        *LogConfig    `mapstructure:"logger"`
	MiscConfig    *MiscConfig   `mapstructure:"misc"`
}

//AuraMQConfig auramq configuration
type AuraMQConfig struct {
	SubscriberBufferSize int      `mapstructure:"subscriber-buffer-size"`
	PingWait             int      `mapstructure:"ping-wait"`
	ReadWait             int      `mapstructure:"read-wait"`
	WriteWait            int      `mapstructure:"write-wait"`
	MinerSyncTopic       string   `mapstructure:"miner-sync-topic"`
	AllSNURLs            []string `mapstructure:"all-sn-urls"`
	Account              string   `mapstructure:"account"`
	PrivateKey           string   `mapstructure:"private-key"`
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
	RecheckingPoolLength        int    `mapstructure:"reckecking-pool-length"`
	RecheckingQueueLength       int    `mapstructure:"reckecking-queue-length"`
	AvaliableNodeTimeGap        int64  `mapstructure:"avaliable-node-time-gap"`
	MinerVersionThreshold       int32  `mapstructure:"miner-version-threshold"`
	PunishPhase1                int32  `mapstructure:"punish-phase1"`
	PunishPhase1Percent         int32  `mapstructure:"punish-phase1-percent"`
	PunishPhase2                int32  `mapstructure:"punish-phase2"`
	PunishPhase2Percent         int32  `mapstructure:"punish-phase2-percent"`
	PunishPhase3                int32  `mapstructure:"punish-phase3"`
	PunishPhase3Percent         int32  `mapstructure:"punish-phase3-percent"`
	SpotCheckSkipTime           int64  `mapstructure:"spotcheck-skip-time"`
	SpotCheckInterval           int64  `mapstructure:"spotcheck-interval"`
	SpotCheckConnectTimeout     int64  `mapstructure:"spotcheck-connect-timeout"`
	ErrorNodePercentThreshold   int32  `mapstructure:"error-node-percent-threshold"`
	PoolErrorMinerTimeThreshold int32  `mapstructure:"pool-error-miner-time-threshold"`
	ExcludeAddrPrefix           string `mapstructure:"exclude-addr-prefix"`
}
