package config

import (
	"fmt"
	"github.com/spf13/viper"
)

type DB struct {
	Host string
	Port int
	Name string
}

type Config struct {
	DB DB
}

func Load() (*Config, error) {
	v := viper.New()
	v.AddConfigPath("./config")
	v.SetConfigName("config")
	v.SetConfigType("yaml")
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}
	// Defaults
	if cfg.DB.Host == "" {
		cfg.DB.Host = v.GetString("DB_HOST")
	}
	if cfg.DB.Port == 0 {
		cfg.DB.Port = v.GetInt("DB_PORT")
	}
	if cfg.DB.Name == "" {
		cfg.DB.Name = v.GetString("DB_NAME")
	}
	if cfg.DB.Host == "" {
		cfg.DB.Host = "127.0.0.1"
	}
	if cfg.DB.Port == 0 {
		cfg.DB.Port = 6534
	}
	if cfg.DB.Name == "" {
		cfg.DB.Name = "testdb"
	}
	return &cfg, nil
}
