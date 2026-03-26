package redismq

import (
	"context"
	"crypto/tls"
	"strings"
	"time"

	_ "github.com/joepeak/golib-conf"

	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	RedisClient redis.UniversalClient
)

func init() {
	if !viper.IsSet("mq.redismq") {
		return
	}

	if client, err := NewRedisClient(); err != nil {
		logrus.Fatal("Failed to initialize Redis client: ", err)
	} else {
		RedisClient = client
	}
}

// NewRedisClient 初始化 Redis 连接
func NewRedisClient() (redis.UniversalClient, error) {
	if viper.GetBool("mq.redismq.redis.enabledCluster") {
		return newClusterClient()
	}
	return newSingleClient()
}

// 初始化单机模式 Redis
func newSingleClient() (redis.UniversalClient, error) {
	opts := &redis.Options{
		Addr:      viper.GetString("mq.redismq.redis.addr"),
		Password:  viper.GetString("mq.redismq.redis.password"),
		DB:        viper.GetInt("mq.redismq.redis.db"),
		TLSConfig: getTLSConfig(),
	}

	client := redis.NewClient(opts)

	if err := pingRedis(client); err != nil {
		logrus.Error("Failed to connect to Redis: ", err, ", addr: ", opts.Addr)
		return nil, err
	}

	logrus.Info("Connected to RedisMQ successfully, addr: ", opts.Addr, ", db: ", opts.DB)
	return client, nil
}

// 初始化 Redis Cluster 模式
func newClusterClient() (redis.UniversalClient, error) {
	addrs := viper.GetStringSlice("mq.redismq.redis.cluster.addrs")
	opts := &redis.ClusterOptions{
		Addrs:     addrs,
		Password:  viper.GetString("mq.redismq.redis.password"),
		TLSConfig: getTLSConfig(),
	}

	client := redis.NewClusterClient(opts)

	if err := pingRedis(client); err != nil {
		logrus.Error("Failed to connect to Redis Cluster: ", err, ", addrs: ", strings.Join(addrs, ","))
		return nil, err
	}

	logrus.Info("Connected to RedisMQ Cluster successfully, addrs: ", strings.Join(addrs, ","))
	return client, nil
}

// TLS 配置
func getTLSConfig() *tls.Config {
	if viper.GetBool("mq.redismq.redis.enabledTls") {
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	return nil
}

// Redis 连接测试
func pingRedis(client redis.UniversalClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return client.Ping(ctx).Err()
}
