package main

import (
	"flag"
	"fmt"
	"github.com/dawnzzz/lmq/config"
	"github.com/dawnzzz/lmq/lmqlookup"
	"os"
)

const banner = `
  ___       _____ ______   ________           ___       ________  ________  ___  __    ___  ___  ________   
|\  \     |\   _ \  _   \|\   __  \         |\  \     |\   __  \|\   __  \|\  \|\  \ |\  \|\  \|\   __  \  
\ \  \    \ \  \\\__\ \  \ \  \|\  \        \ \  \    \ \  \|\  \ \  \|\  \ \  \/  /|\ \  \\\  \ \  \|\  \ 
 \ \  \    \ \  \\|__| \  \ \  \\\  \        \ \  \    \ \  \\\  \ \  \\\  \ \   ___  \ \  \\\  \ \   ____\
  \ \  \____\ \  \    \ \  \ \  \\\  \        \ \  \____\ \  \\\  \ \  \\\  \ \  \\ \  \ \  \\\  \ \  \___|
   \ \_______\ \__\    \ \__\ \_____  \        \ \_______\ \_______\ \_______\ \__\\ \__\ \_______\ \__\   
    \|_______|\|__|     \|__|\|___| \__\        \|_______|\|_______|\|_______|\|__| \|__|\|_______|\|__|   
                                   \|__|                                                                   
powered by https://github.com/dawnzzz/lmq
`

func init() {
	// 加载配置信息
	var configFilename string
	configFilename = *flag.String("f", config.DefaultLmqLookupFilename, "LMQ Daemon yaml config file")
	_, err := os.Stat(configFilename)
	if err != nil && !os.IsNotExist(err) {
		panic(err)
	} else if err != nil {
		configFilename = config.DefaultLmqLookupFilename
		_, err = os.Stat(configFilename)
		if err != nil {
			panic(err)
		}
	}
	err = config.LoadConfigFile(configFilename, &config.GlobalLmqdConfig)
	if err != nil {
		panic(err)
	}
}

func main() {
	fmt.Print(banner)

	lookup := lmqlookup.NewLmqLookUp()

	lookup.Main()
}
