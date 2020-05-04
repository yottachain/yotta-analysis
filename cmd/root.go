/*
Package cmd created by cobra framework
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
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
	"github.com/yottachain/yotta-analysis/pb"
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
		if len(config.MongoDBURLs) != int(config.SNCount) {
			panic("count of mongoDB URL is not equal to SN count\n")
		}
		initLog(config)
		analyser, err := ytanalysis.New(config.MongoDBURLs, config.DBNameIndexed, config.AnalysisDBURL, config.EOS.URL, config.EOS.BPAccount, config.EOS.BPPrivateKey, config.EOS.ContractOwnerM, config.EOS.ContractOwnerD, config.EOS.ShadowAccount, config.SNCount, config.MiscConfig)
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting analyser: %s\n", err))
		}
		analyser.StartRecheck()
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

	viper.AutomaticEnv() // read in environment variables that match

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
	//DefaultMongoDBURLS default value of MongoDBURLS
	DefaultMongoDBURLS []string = []string{"mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct", "mongodb://127.0.0.1:27017/?connect=direct"}
	//DefaultDBNameIndexed default value of DBNameIndexed
	DefaultDBNameIndexed bool = false
	//DefaultSNCount default value of SNCount
	DefaultSNCount int64 = 21

	//DefaultEOSURL default value of EOSURL
	DefaultEOSURL string = "http://127.0.0.1:8888"
	//DefaultEOSBPAccount default value of EOSBPAccount
	DefaultEOSBPAccount string = ""
	//DefaultEOSBPPrivateKey default value of EOSBPPrivateKey
	DefaultEOSBPPrivateKey string = ""
	//DefaultEOSContractOwnerM default value of EOSContractOwnerM
	DefaultEOSContractOwnerM string = ""
	//DefaultEOSContractOwnerD default value of EOSContractOwnerD
	DefaultEOSContractOwnerD string = ""
	//DefaultEOSShadowAccount default value of EOSShadowAccount
	DefaultEOSShadowAccount string = ""

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
	//DefaultMiscSpotCheckSkipTime default value of MiscSpotCheckSkipTime
	DefaultMiscSpotCheckSkipTime int64 = 0
	//DefaultMiscSpotCheckInterval default value of MiscSpotCheckInterval
	DefaultMiscSpotCheckInterval int64 = 60
	//DefaultMiscSpotCheckConnectTimeout default value of MiscSpotCheckConnectTimeout
	DefaultMiscSpotCheckConnectTimeout int64 = 10
	//DefaultMiscErrorNodePercentThreshold default value of MiscErrorNodePercentThreshold
	DefaultMiscErrorNodePercentThreshold int32 = 95
	//DefaultMiscExcludeAddrPrefix default value of MiscExcludeAddrPrefix
	DefaultMiscExcludeAddrPrefix string = ""
)

func initFlag() {
	//main config
	rootCmd.PersistentFlags().String(ytanalysis.BindAddrField, DefaultBindAddr, "Binding address of GRPC server")
	viper.BindPFlag(ytanalysis.BindAddrField, rootCmd.PersistentFlags().Lookup(ytanalysis.BindAddrField))
	rootCmd.PersistentFlags().String(ytanalysis.AnalysisDBURLField, DefaultAnalysisDBURL, "mongoDB URL of analysis database")
	viper.BindPFlag(ytanalysis.AnalysisDBURLField, rootCmd.PersistentFlags().Lookup(ytanalysis.AnalysisDBURLField))
	rootCmd.PersistentFlags().StringSlice(ytanalysis.MongoDBURLSField, DefaultMongoDBURLS, "URLs of SN-syncing database, in the form of --mongodb-urls \"URL1,URL2,URL3\"")
	viper.BindPFlag(ytanalysis.MongoDBURLSField, rootCmd.PersistentFlags().Lookup(ytanalysis.MongoDBURLSField))
	rootCmd.PersistentFlags().Bool(ytanalysis.DBNameIndexedField, DefaultDBNameIndexed, "if value is true, add index number as suffix of each database name")
	viper.BindPFlag(ytanalysis.DBNameIndexedField, rootCmd.PersistentFlags().Lookup(ytanalysis.DBNameIndexedField))
	rootCmd.PersistentFlags().Int64(ytanalysis.SNCountField, DefaultSNCount, "count of SN")
	viper.BindPFlag(ytanalysis.SNCountField, rootCmd.PersistentFlags().Lookup(ytanalysis.SNCountField))
	//EOS config
	rootCmd.PersistentFlags().String(ytanalysis.EOSURLField, DefaultEOSURL, "URL of EOS server")
	viper.BindPFlag(ytanalysis.EOSURLField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSURLField))
	rootCmd.PersistentFlags().String(ytanalysis.EOSBPAccountField, DefaultEOSBPAccount, "Account name of SN")
	viper.BindPFlag(ytanalysis.EOSBPAccountField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSBPAccountField))
	rootCmd.PersistentFlags().String(ytanalysis.EOSBPPrivateKeyField, DefaultEOSBPPrivateKey, "Private key of SN account")
	viper.BindPFlag(ytanalysis.EOSBPPrivateKeyField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSBPPrivateKeyField))
	rootCmd.PersistentFlags().String(ytanalysis.EOSContractOwnerMField, DefaultEOSContractOwnerM, "Account name of contract owner M")
	viper.BindPFlag(ytanalysis.EOSContractOwnerMField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSContractOwnerMField))
	rootCmd.PersistentFlags().String(ytanalysis.EOSContractOwnerDField, DefaultEOSContractOwnerD, "Account name of contract owner D")
	viper.BindPFlag(ytanalysis.EOSContractOwnerDField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSContractOwnerDField))
	rootCmd.PersistentFlags().String(ytanalysis.EOSShadowAccountField, DefaultEOSShadowAccount, "Account name of shadow account")
	viper.BindPFlag(ytanalysis.EOSShadowAccountField, rootCmd.PersistentFlags().Lookup(ytanalysis.EOSShadowAccountField))
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
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase1Field, DefaultMiscPunishPhase1, "When reaching this value in spotchecking, do 1st phase punishment to the miner")
	viper.BindPFlag(ytanalysis.MiscPunishPhase1Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase1Field))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase2Field, DefaultMiscPunishPhase2, "When reaching this value in spotchecking, do 2nd phase punishment to the miner")
	viper.BindPFlag(ytanalysis.MiscPunishPhase2Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase2Field))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase3Field, DefaultMiscPunishPhase3, "When reaching this value in spotchecking, do 3rd phase punishment to the miner")
	viper.BindPFlag(ytanalysis.MiscPunishPhase3Field, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase3Field))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase1PercentField, DefaultMiscPunishPhase1Percent, "Percentage of 1st phase desposit punishing")
	viper.BindPFlag(ytanalysis.MiscPunishPhase1PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase1PercentField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase2PercentField, DefaultMiscPunishPhase2Percent, "Percentage of 2nd phase desposit punishing")
	viper.BindPFlag(ytanalysis.MiscPunishPhase2PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase2PercentField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscPunishPhase3PercentField, DefaultMiscPunishPhase3Percent, "Percentage of 3rd phase desposit punishing")
	viper.BindPFlag(ytanalysis.MiscPunishPhase3PercentField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscPunishPhase3PercentField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckSkipTimeField, DefaultMiscSpotCheckSkipTime, "Shards uploaded before this timestamp wil not be spotchecked")
	viper.BindPFlag(ytanalysis.MiscSpotCheckSkipTimeField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckSkipTimeField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckIntervalField, DefaultMiscSpotCheckInterval, "Each miner will be spotchecked in this interval")
	viper.BindPFlag(ytanalysis.MiscSpotCheckIntervalField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckIntervalField))
	rootCmd.PersistentFlags().Int64(ytanalysis.MiscSpotCheckConnectTimeoutField, DefaultMiscSpotCheckConnectTimeout, "Timeout of connecting to miner when spotchecking")
	viper.BindPFlag(ytanalysis.MiscSpotCheckConnectTimeoutField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscSpotCheckConnectTimeoutField))
	rootCmd.PersistentFlags().Int32(ytanalysis.MiscErrorNodePercentThresholdField, DefaultMiscErrorNodePercentThreshold, "Percentage of valid miner in th pool, bigger then which punishment will be skipped")
	viper.BindPFlag(ytanalysis.MiscErrorNodePercentThresholdField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscErrorNodePercentThresholdField))
	rootCmd.PersistentFlags().String(ytanalysis.MiscExcludeAddrPrefixField, DefaultMiscExcludeAddrPrefix, "Miners with this value as address prefix is considered as valid")
	viper.BindPFlag(ytanalysis.MiscExcludeAddrPrefixField, rootCmd.PersistentFlags().Lookup(ytanalysis.MiscExcludeAddrPrefixField))
}
