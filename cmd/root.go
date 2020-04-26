/*
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
		config := ytanalysis.InitializeConfig()
		initLog(config)
		analyser, err := ytanalysis.New(config.MongoDBURL, config.EOS.URL, config.EOS.BPAccount, config.EOS.BPPrivateKey, config.EOS.ContractOwnerM, config.EOS.ContractOwnerD, config.EOS.ShadowAccount, config.SNCount, config.MiscConfig)
		if err != nil {
			panic(fmt.Sprintf("fatal error when starting analyser: %s\n", err))
		}
		analyser.StartRecheck()
		lis, err := net.Listen("tcp", config.BindAddr)
		if err != nil {
			log.Fatalf("failed to listen address %s: %s\n", config.BindAddr, err)
		}
		log.Printf("GRPC address: %s\n", config.BindAddr)
		grpcServer := grpc.NewServer()
		server := &ytanalysis.Server{Analyser: analyser, Timeout: config.Timeout}
		pb.RegisterAnalysisServer(grpcServer, server)
		grpcServer.Serve(lis)
		log.Info("GRPC server started")
	},
}

func initLog(config *ytanalysis.Config) {
	/* 日志轮转相关函数
	`WithLinkName` 为最新的日志建立软连接
	`WithRotationTime` 设置日志分割的时间，隔多久分割一次
	WithMaxAge 和 WithRotationCount二者只能设置一个
	 `WithMaxAge` 设置文件清理前的最长保存时间
	 `WithRotationCount` 设置文件清理前最多保存的个数
	*/
	// 下面配置日志每隔 1 分钟轮转一个新文件，保留最近 3 分钟的日志文件，多余的自动清理掉。
	writer, _ := rotatelogs.New(
		config.Logger.FilePath+".%Y%m%d",
		rotatelogs.WithLinkName(config.Logger.FilePath),
		rotatelogs.WithMaxAge(time.Duration(config.Logger.MaxAge)*time.Hour),
		rotatelogs.WithRotationTime(time.Duration(config.Logger.RotationTime)*time.Hour),
	)
	log.SetOutput(writer)
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
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
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
		}
		os.Exit(1)
	}
}
