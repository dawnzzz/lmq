package config

import "github.com/spf13/viper"

const (
	DefaultLmqdFilename      = "lmqd.yaml"
	DefaultLmqLookupFilename = "lookup.yaml"
)

func LoadConfigFile(filename string, v interface{}) error {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(filename)
	err := viper.ReadInConfig()
	if err != nil {
		return err
	}

	if err = viper.Unmarshal(&v); err != nil {
		return err
	}

	return nil
}
