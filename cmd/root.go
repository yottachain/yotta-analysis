package cmd

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	homedir "github.com/mitchellh/go-homedir"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	ytanalysis "github.com/yottachain/yotta-analysis"
	pb "github.com/yottachain/yotta-analysis/pbanalysis"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "yotta-analysis",
	Short: "analysis platform of YottaChain",
	Long:  `yotta-analysis is an analysis platform performing data checking/repairing tasks, etc.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		config := new(ytanalysis.Config)
		if err := viper.Unmarshal(config); err != nil {
			panic(fmt.Sprintf("unable to decode into config struct, %v\n", err))
		}
		// if len(config.MongoDBURLs) != int(config.SNCount) {
		// 	panic("count of mongoDB URL is not equal to SN count\n")
		// }
		initLog(config)
		ctx := context.Background()
		analyser, err := ytanalysis.New(ctx, config.AnalysisDBURL, config.PDURLs, config.ESURLs, config.ESUserName, config.ESPassword, config.MinerTrackerURL, config.MinerStat, config.MiscConfig)
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting analyser: %s\n", err))
		}
		analyser.StartRecheck(ctx)
		analyser.TrackingStat(ctx)
		lis, err := net.Listen("tcp", config.BindAddr)
		if err != nil {
			log.Fatalf("failed to listen address %s: %s\n", config.BindAddr, err)
		}
		log.Infof("GRPC address: %s", config.BindAddr)
		grpcServer := grpc.NewServer()
		server := &ytanalysis.Server{Analyser: analyser}
		pb.RegisterAnalysisServer(grpcServer, server)
		grpcServer.Serve(lis)
		log.Info("GRPC server started")
	},
}

func initLog(config *ytanalysis.Config) {
	switch strings.ToLower(config.Logger.Output) {
	case "file":
		writer, _ := rotatelogs.New(
			config.Logger.FilePath+".%Y%m%d",
			rotatelogs.WithLinkName(config.Logger.FilePath),
			rotatelogs.WithMaxAge(time.Duration(config.Logger.MaxAge)*time.Hour),
			rotatelogs.WithRotationTime(time.Duration(config.Logger.RotationTime)*time.Hour),
		)
		log.SetOutput(writer)
	case "stdout":
		log.SetOutput(os.Stdout)
	default:
		fmt.Printf("no such option: %s, use stdout\n", config.Logger.Output)
		log.SetOutput(os.Stdout)
	}
	log.SetFormatter(&log.TextFormatter{})
	levelMap := make(map[string]log.Level)
	levelMap["panic"] = log.PanicLevel
	levelMap["fatal"] = log.FatalLevel
	levelMap["error"] = log.ErrorLevel
	levelMap["warn"] = log.WarnLevel
	levelMap["info"] = log.InfoLevel
	levelMap["debug"] = log.DebugLevel
	levelMap["trace"] = log.TraceLevel
	log.SetLevel(levelMap[strings.ToLower(config.Logger.Level)])
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/yotta-analysis.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	initFlag()
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".yotta-analysis" (without extension).
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigName("yotta-analysis")
		viper.SetConfigType("yaml")
	}

	// viper.AutomaticEnv() // read in environment variables that match
	// viper.SetEnvPrefix("analysis")
	// viper.SetEnvKeyReplacer(strings.NewReplacer("_", "."))

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			fmt.Println("Config file not found.")
		} else {
			// Config file was found but another error was produced
			fmt.Println("Error:", err.Error())
			os.Exit(1)
		}
	}
}

var (
	//DefaultBindAddr default value of BindAddr
	DefaultBindAddr string = ":8080"
	//DefaultAnalysisDBURL default value of AnalysisDBURL
	DefaultAnalysisDBURL string = "mongodb://127.0.0.1:27017/?connect=direct"
	//DefaultPDURLs default value of PDURLs
	DefaultPDURLs []string = []string{"127.0.0.1:2379"}
	//DefaultESURLs default value of ESURLs
	DefaultESURLs []string = []string{"http://127.0.0.1:9200"}
	//DefaultESUserName default value of ESUserName
	DefaultESUserName string = ""
	//DefaultESPassword default value of ESPassword
	DefaultESPassword string = ""
	//DefaultMinerTrackerURL default value of MinerTrackerURL
	DefaultMinerTrackerURL string = "https://dnrpc.yottachain.net/query"

	// //DefaultAuramqSubscriberBufferSize default value of AuramqSubscriberBufferSize
	// DefaultAuramqSubscriberBufferSize = 1024
	// //DefaultAuramqPingWait default value of AuramqPingWait
	// DefaultAuramqPingWait = 30
	// //DefaultAuramqReadWait default value of AuramqReadWait
	// DefaultAuramqReadWait = 60
	// //DefaultAuramqWriteWait default value of AuramqWriteWait
	// DefaultAuramqWriteWait = 10
	// //DefaultAuramqMinerSyncTopic default value of AuramqMinerSyncTopic
	// DefaultAuramqMinerSyncTopic = "sync"
	// //DefaultAuramqAllSNURLs default value of AuramqAllSNURLs
	// DefaultAuramqAllSNURLs = []string{}
	// //DefaultAuramqAccount default value of AuramqAccount
	// DefaultAuramqAccount = ""
	// //DefaultAuramqPrivateKey default value of AuramqPrivateKey
	// DefaultAuramqPrivateKey = ""
	// //DefaultAuramqClientID default value of AuramqClientID
	// DefaultAuramqClientID = "yottaanalysis"

	//DefaultMinerStatAllSyncURLs default value of MinerStatAllSyncURLs
	DefaultMinerStatAllSyncURLs = []string{}
	//DefaultMinerStatBatchSize default value of MinerStatBatchSize
	DefaultMinerStatBatchSize = 100
	//DefaultMinerStatWaitTime default value of MinerStatWaitTime
	DefaultMinerStatWaitTime = 10
	//DefaultMinerStatSkipTime default value of MinerStatSkipTime
	DefaultMinerStatSkipTime = 180

	//DefaultLoggerOutput default value of LoggerOutput
	DefaultLoggerOutput string = "stdout"
	//DefaultLoggerFilePath default value of LoggerFilePath
	DefaultLoggerFilePath string = "./spotcheck.log"
	//DefaultLoggerRotationTime default value of LoggerRotationTime
	DefaultLoggerRotationTime int64 = 24
	//DefaultLoggerMaxAge default value of LoggerMaxAge
	DefaultLoggerMaxAge int64 = 240
	//DefaultLoggerLevel default value of LoggerLevel
	DefaultLoggerLevel string = "Info"

	//DefaultMiscRecheckingPoolLength default value of MiscRecheckingPoolLength
	DefaultMiscRecheckingPoolLength int = 5000
	//DefaultMiscRecheckingQueueLength default value of MiscRecheckingQueueLength
	DefaultMiscRecheckingQueueLength int = 10000
	//DefaultMiscAvaliableNodeTimeGap default value of MiscAvaliableNodeTimeGap
	DefaultMiscAvaliableNodeTimeGap int64 = 3
	//DefaultMiscMinerVersionThreshold default value of MiscMinerVersionThreshold
	DefaultMiscMinerVersionThreshold int32 = 0
	//DefaultMiscPunishPhase1 default value of MiscPunishPhase1
	DefaultMiscPunishPhase1 int32 = 4
	//DefaultMiscPunishPhase2 default value of MiscPunishPhase2
	DefaultMiscPunishPhase2 int32 = 24
	//DefaultMiscPunishPhase3 default value of MiscPunishPhase3
	DefaultMiscPunishPhase3 int32 = 168
	//DefaultMiscPunishPhase1Percent default value of MiscPunishPhase1Percent
	DefaultMiscPunishPhase1Percent int32 = 1
	//DefaultMiscPunishPhase2Percent default value of MiscPunishPhase2Percent
	DefaultMiscPunishPhase2Percent int32 = 10
	//DefaultMiscPunishPhase3Percent default value of MiscPunishPhase3Percent
	DefaultMiscPunishPhase3Percent int32 = 50
	//DefaultMiscSpotCheckStartTime default value of MiscSpotCheckStartTime
	DefaultMiscSpotCheckStartTime int64 = 0
	//DefaultMiscSpotCheckEndTime default value of MiscSpotCheckEndTime
	DefaultMiscSpotCheckEndTime int64 = 0
	//DefaultMiscSpotCheckInterval default value of MiscSpotCheckInterval
	DefaultMiscSpotCheckInterval int64 = 60
	//DefaultMiscSpotCheckConnectTimeout default value of MiscSpotCheckConnectTimeout
	DefaultMiscSpotCheckConnectTimeout int64 = 10
	//DefaultMiscErrorNodePercentThreshold default value of MiscErrorNodePercentThreshold
	//DefaultMiscErrorNodePercentThreshold int32 = 95
	//DefaultMiscPoolErrorMinerTimeThreshold default value of MiscPoolErrorMinerTimeThreshold
	//DefaultMiscPoolErrorMinerTimeThreshold int32 = 14400
	//DefaultMiscExcludeAddrPrefix default value of MiscExcludeAddrPrefix
	DefaultMiscExcludeAddrPrefix string = ""
	//DefaultMiscRetryTimeDealy default value of MiscRetryTimeDelay
	DefaultMiscRetryTimeDealy int32 = 5000
	//DefaultMiscMinerTrackBatchSize default value of MiscMinerTrackBatchSize
	DefaultMiscMinerTrackBatchSize int32 = 2000
	//DefaultMiscMinerTrackInterval default value of MiscMinerTrackInterval
	DefaultMiscMinerTrackInterval int32 = 2
	//DefaultMiscPrepareTaskPoolSize default value of MiscPrepareTaskPoolSize
	DefaultMiscPrepareTaskPoolSize int32 = 1000
	//DefaultMiscCalculateCDInterval default value of MiscCalculateCDInterval
	DefaultMiscCalculateCDInterval int32 = 3
)

func initFlag() {
	//main config
	rootCmd.PersistentFlags().String(ytanalysis.BindAddrField, DefaultBindAddr, "Binding address of GRPC server")
	viper.BindPFlag(ytanalysis.BindAddrField, rootCmd.PersistentFlags().Lookup(ytanalysis.BindAddrField))
	rootCmd.PersistentFlags().String(ytanalysis.AnalysisDBURLField, DefaultAnalysisDBURL, "mongoDB URL of analysis database")
	viper.BindPFlag(ytanalysis.AnalysisDBURLField, rootCmd.PersistentFlags().Lookup(ytanalysis.AnalysisDBURLField))
	rootCmd.PersistentFlags().StringSlice(ytanalysis.PDURLsField, DefaultPDURLs, "URLs of PD")
	viper.BindPFlag(ytanalysis.PDURLsField, rootCmd.PersistentFlags().Lookup(ytanalysis.PDURLsField))
	rootCmd.PersistentFlags().StringSlice(ytanalysis.ESURLsField, DefaultESURLs, "URLs of ES")
	viper.BindPFlag(ytanalysis.ESURLsField, rootCmd.PersistentFlags().Lookup(ytanalysis.ESURLsField))
	rootCmd.PersistentFlags().String(ytanalysis.ESUserNameField, DefaultESUserName, "username of elasticsearch")
	viper.BindPFlag(ytanalysis.ESUserNameField, rootCmd.PersistentFlags().Lookup(ytanalysis.ESUserNameField))
	rootCmd.PersistentFlags().String(ytanalysis.ESPasswordField, DefaultESPassword, "password of elasticsearch")
	viper.BindPFlag(ytanalysis.ESPasswordField, rootCmd.PersistentFlags().Lookup(ytanalysis.ESPasswordField))
	rootCmd.PersistentFlags().String(ytanalysis.MinerTrackerURLField, DefaultMinerTrackerURL, "URL of miner tracker service")
	viper.BindPFlag(ytanalysis.MinerTrackerURLField, rootCmd.PersistentFlags().Lookup(ytanalysis.MinerTrackerURLField))
	// //AuraMQ config
	// rootCmd.PersistentFlags().Int(ytanalysis.AuramqSubscriberBufferSizeField, DefaultAuramqSubscriberBufferSize, "subscriber buffer size")
	// viper.BindPFlag(ytanalysis.AuramqSubscriberBufferSizeField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqSubscriberBufferSizeField))
	// rootCmd.PersistentFlags().Int(ytanalysis.AuramqPingWaitField, DefaultAuramqPingWait, "ping interval of MQ client")
	// viper.BindPFlag(ytanalysis.AuramqPingWaitField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqPingWaitField))
	// rootCmd.PersistentFlags().Int(ytanalysis.AuramqReadWaitField, DefaultAuramqReadWait, "read wait of MQ client")
	// viper.BindPFlag(ytanalysis.AuramqReadWaitField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqReadWaitField))
	// rootCmd.PersistentFlags().Int(ytanalysis.AuramqWriteWaitField, DefaultAuramqWriteWait, "write wait of MQ client")
	// viper.BindPFlag(ytanalysis.AuramqWriteWaitField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqWriteWaitField))
	// rootCmd.PersistentFlags().String(ytanalysis.AuramqMinerSyncTopicField, DefaultAuramqMinerSyncTopic, "miner sync topic name")
	// viper.BindPFlag(ytanalysis.AuramqMinerSyncTopicField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqMinerSyncTopicField))
	// rootCmd.PersistentFlags().StringSlice(ytanalysis.AuramqAllSNURLsField, DefaultAuramqAllSNURLs, "all URLs of MQ port, in the form of --auramq.all-sn-urls \"URL1,URL2,URL3\"")
	// viper.BindPFlag(ytanalysis.AuramqAllSNURLsField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqAllSNURLsField))
	// rootCmd.PersistentFlags().String(ytanalysis.AuramqAccountField, DefaultAuramqAccount, "BP account for anthentication")
	// viper.BindPFlag(ytanalysis.AuramqAccountField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqAccountField))
	// rootCmd.PersistentFlags().String(ytanalysis.AuramqPrivateKeyField, DefaultAuramqPrivateKey, "")
	// viper.BindPFlag(ytanalysis.AuramqPrivateKeyField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqPrivateKeyField))
	// rootCmd.PersistentFlags().String(ytanalysis.AuramqClientIDField, DefaultAuramqClientID, "client ID for identifying MQ client")
	// viper.BindPFlag(ytanalysis.AuramqClientIDField, rootCmd.PersistentFlags().Lookup(ytanalysis.AuramqClientIDField))
	//MinerStat config
	rootCmd.PersistentFlags().StringSlice(ytanalysis.MinerStatAllSyncURLsField, DefaultMinerStatAllSyncURLs, "all URLs of sync services, in the form of --miner-stat.all-sync-urls \"URL1,URL2,URL3\"")
	viper.BindPFlag(ytanalysis.MinerStatAllSyncURLsField, rootCmd.PersistentFlags().Lookup(ytanalysis.MinerStatAllSyncURLsField))
	rootCmd.PersistentFlags().Int(ytanalysis.MinerStatBatchSizeField, DefaultMinerStatBatchSize, "batch size when fetching miner logs")
	viper.BindPFlag(ytanalysis.MinerStatBatchSizeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MinerStatBatchSizeField))
	rootCmd.PersistentFlags().Int(ytanalysis.MinerStatWaitTimeField, DefaultMinerStatWaitTime, "wait time when no new miner logs can be fetched")
	viper.BindPFlag(ytanalysis.MinerStatWaitTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MinerStatWaitTimeField))
	rootCmd.PersistentFlags().Int(ytanalysis.MinerStatSkipTimeField, DefaultMinerStatSkipTime, "ensure not to fetching miner logs till the end")
	viper.BindPFlag(ytanalysis.MinerStatSkipTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MinerStatSkipTimeField))
	//logger config
	rootCmd.PersistentFlags().String(ytanalysis.LoggerOutputField, DefaultLoggerOutput, "Output type of logger(stdout or file)")
	viper.BindPFlag(ytanalysis.LoggerOutputField, rootCmd.PersistentFlags().Lookup(ytanalysis.LoggerOutputField))
	rootCmd.PersistentFlags().String(ytanalysis.LoggerFilePathField, DefaultLoggerFilePath, "Output path of log file")
	viper.BindPFlag(ytanalysis.LoggerFilePathField, rootCmd.PersistentFlags().Lookup(ytanalysis.LoggerFilePathField))
	rootCmd.PersistentFlags().Int64(ytanalysis.LoggerRotationTimeField, DefaultLoggerRotationTime, "Rotation time(hour) of log file")
	viper.BindPFlag(ytanalysis.LoggerRotationTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.LoggerRotationTimeField))
	rootCmd.PersistentFlags().Int64(ytanalysis.LoggerMaxAgeField, DefaultLoggerMaxAge, "Within the time(hour) of this value each log file will be kept")
	viper.BindPFlag(ytanalysis.LoggerMaxAgeField, rootCmd.PersistentFlags().Lookup(ytanalysis.LoggerMaxAgeField))
	rootCmd.PersistentFlags().String(ytanalysis.LoggerLevelField, DefaultLoggerLevel, "Log level(Trace, Debug, Info, Warning, Error, Fatal, Panic)")
	viper.BindPFlag(ytanalysis.LoggerLevelField, rootCmd.PersistentFlags().Lookup(ytanalysis.LoggerLevelField))
	//Misc config
	rootCmd.PersistentFlags().Int(ytanalysis.MiscRecheckingPoolLengthField, DefaultMiscRecheckingPoolLength, "Length of rechecking task pool")
	viper.BindPFlag(ytanalysis.MiscRecheckingPoolLengthField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscRecheckingPoolLengthField))
	rootCmd.PersistentFlags().Int(ytanalysis.MiscRecheckingQueueLengthField, DefaultMiscRecheckingQueueLength, "Length of rechecking task queue, in which idle tasks are waiting for scheduling")
	viper.BindPFlag(ytanalysis.MiscRecheckingQueueLengthField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscRecheckingQueueLengthField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscAvaliableNodeTimeGapField, DefaultMiscAvaliableNodeTimeGap, "Reporting interval time(minute), under this value miner is considered as active")
	viper.BindPFlag(ytanalysis.MiscAvaliableNodeTimeGapField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscAvaliableNodeTimeGapField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscMinerVersionThresholdField, DefaultMiscMinerVersionThreshold, "Miner is considered as valid whose version is not less than this value")
	viper.BindPFlag(ytanalysis.MiscMinerVersionThresholdField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscMinerVersionThresholdField))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase1Field, DefaultMiscPunishPhase1, "When reaching this value in spotchecking, do 1st phase punishment to the miner")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase1Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase1Field))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase2Field, DefaultMiscPunishPhase2, "When reaching this value in spotchecking, do 2nd phase punishment to the miner")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase2Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase2Field))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase3Field, DefaultMiscPunishPhase3, "When reaching this value in spotchecking, do 3rd phase punishment to the miner")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase3Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase3Field))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase1PercentField, DefaultMiscPunishPhase1Percent, "Percentage of 1st phase desposit punishing")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase1PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase1PercentField))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase2PercentField, DefaultMiscPunishPhase2Percent, "Percentage of 2nd phase desposit punishing")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase2PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase2PercentField))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase3PercentField, DefaultMiscPunishPhase3Percent, "Percentage of 3rd phase desposit punishing")
	// viper.BindPFlag(ytanalysis.MiscPunishPhase3PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase3PercentField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckStartTimeField, DefaultMiscSpotCheckStartTime, "Shards uploaded before this timestamp wil not be spotchecked")
	viper.BindPFlag(ytanalysis.MiscSpotCheckStartTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckStartTimeField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckEndTimeField, DefaultMiscSpotCheckEndTime, "Shards uploaded after this timestamp wil not be spotchecked")
	viper.BindPFlag(ytanalysis.MiscSpotCheckEndTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckEndTimeField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckIntervalField, DefaultMiscSpotCheckInterval, "Each miner will be spotchecked in this interval")
	viper.BindPFlag(ytanalysis.MiscSpotCheckIntervalField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckIntervalField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckConnectTimeoutField, DefaultMiscSpotCheckConnectTimeout, "Timeout of connecting to miner when spotchecking")
	viper.BindPFlag(ytanalysis.MiscSpotCheckConnectTimeoutField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckConnectTimeoutField))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscErrorNodePercentThresholdField, DefaultMiscErrorNodePercentThreshold, "Percentage of valid miner in th pool, bigger then which punishment will be skipped")
	// viper.BindPFlag(ytanalysis.MiscErrorNodePercentThresholdField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscErrorNodePercentThresholdField))
	// rootCmd.PersistentFlags().Int32(ytanalysis.MiscPoolErrorMinerTimeThresholdField, DefaultMiscPoolErrorMinerTimeThreshold, "Miner with down time greater than this value is considered as invalid miner during pool weight related calculation")
	// viper.BindPFlag(ytanalysis.MiscPoolErrorMinerTimeThresholdField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPoolErrorMinerTimeThresholdField))
	rootCmd.PersistentFlags().String(ytanalysis.MiscExcludeAddrPrefixField, DefaultMiscExcludeAddrPrefix, "Miners with this value as address prefix is considered as valid")
	viper.BindPFlag(ytanalysis.MiscExcludeAddrPrefixField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscExcludeAddrPrefixField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscRetryTimeDelayField, DefaultMiscRetryTimeDealy, "delay time between retries")
	viper.BindPFlag(ytanalysis.MiscRetryTimeDelayField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscRetryTimeDelayField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscMinerTrackBatchSizeField, DefaultMiscMinerTrackBatchSize, "batch size when fetching miners from minertracker service")
	viper.BindPFlag(ytanalysis.MiscMinerTrackBatchSizeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscMinerTrackBatchSizeField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscMinerTrackIntervalField, DefaultMiscMinerTrackInterval, "interval time between fetching miners from minertracker service")
	viper.BindPFlag(ytanalysis.MiscMinerTrackIntervalField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscMinerTrackIntervalField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPrepareTaskPoolSizeField, DefaultMiscPrepareTaskPoolSize, "pool size of preparing tasks")
	viper.BindPFlag(ytanalysis.MiscPrepareTaskPoolSizeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPrepareTaskPoolSizeField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscCalculateCDIntervalField, DefaultMiscCalculateCDInterval, "interval time between calculating c and d parameters")
	viper.BindPFlag(ytanalysis.MiscCalculateCDIntervalField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscCalculateCDIntervalField))
}
