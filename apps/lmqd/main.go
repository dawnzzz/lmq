package main

import (
	"flag"
	"fmt"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/lmqd"
	"os"
)

const banner = `
 ___       _____ ______   ________           ________  ________  _______   _____ ______   ________  ________      
|\  \     |\   _ \  _   \|\   __  \         |\   ___ \|\   __  \|\  ___ \ |\   _ \  _   \|\   __  \|\   ___  \    
\ \  \    \ \  \\\__\ \  \ \  \|\  \        \ \  \_|\ \ \  \|\  \ \   __/|\ \  \\\__\ \  \ \  \|\  \ \  \\ \  \   
 \ \  \    \ \  \\|__| \  \ \  \\\  \        \ \  \ \\ \ \   __  \ \  \_|/_\ \  \\|__| \  \ \  \\\  \ \  \\ \  \  
  \ \  \____\ \  \    \ \  \ \  \\\  \        \ \  \_\\ \ \  \ \  \ \  \_|\ \ \  \    \ \  \ \  \\\  \ \  \\ \  \ 
   \ \_______\ \__\    \ \__\ \_____  \        \ \_______\ \__\ \__\ \_______\ \__\    \ \__\ \_______\ \__\\ \__\
    \|_______|\|__|     \|__|\|___| \__\        \|_______|\|__|\|__|\|_______|\|__|     \|__|\|_______|\|__| \|__|
                                   \|__|
powered by https://github.com/dawnzzz/lmq
`

func main() {
	// 加载配置信息
	var configFilename string
	configFilename = *flag.String("f", config.DefaultLmqdFilename, "LMQ Daemon yaml config file")
	_, err := os.Stat(configFilename)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	} else if err != nil {
		configFilename = config.DefaultLmqdFilename
		_, err = os.Stat(configFilename)
		if err != nil {
			panic(err)
		}
	}
	err = config.LoadConfigFile(configFilename, &config.GlobalLmqdConfig)
	if err != nil {
		panic(err)
	}

	fmt.Print(banner)

	_, err = os.Stat(config.GlobalLmqdConfig.DataRootPath)
	if os.IsNotExist(err) {
		innerErr := os.MkdirAll(config.GlobalLmqdConfig.DataRootPath, 0600)
		if innerErr != nil {
			panic(err)
		}
	} else if err != nil {
		panic(err)
	}

	lmqDaemon, err := lmqd.NewLmqDaemon()
	if err != nil {
		panic(err)
	}

	// 加载元数据，持久化元数据，检查是否正常
	err = lmqDaemon.LoadMetaData()
	if err != nil {
		panic(err)
	}
	err = lmqDaemon.PersistMetaData()
	if err != nil {
		panic(err)
	}

	lmqDaemon.Main()
}
