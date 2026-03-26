package wmkafka

import (
	"context"
	"fmt"

	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/spf13/viper"
)

type WmKafkaManager struct {
	subscriber *kafka.Subscriber
	publisher  *kafka.Publisher
	config     *WmKafkaConfig
}

type WmKafkaConfig struct {
	Watermill WatermillConfig `json:"watermill" yaml:"watermill" mapstructure:"watermill"`
	Kafka     KafkaConfig     `json:"kafkamq" yaml:"kafkamq" mapstructure:"kafkamq"`
}

type WatermillConfig struct {
	Logger LoggerConfig `json:"logger" yaml:"logger" mapstructure:"logger"`
}

type LoggerConfig struct {
	Debug bool `json:"debug" yaml:"debug" mapstructure:"debug"`
	Trace bool `json:"trace" yaml:"trace" mapstructure:"trace"`
}

type KafkaConfig struct {
	Consumer KafkaConsumerConfig `json:"consumer" yaml:"consumer" mapstructure:"consumer"`
	TLS      KafkaTLSConfig      `json:"tls" yaml:"tls" mapstructure:"tls"`
	SASL     KafkaSASLConfig     `json:"sasl" yaml:"sasl" mapstructure:"sasl"`
	Brokers  []string            `json:"brokers" yaml:"brokers" mapstructure:"brokers"`
}

type KafkaConsumerConfig struct {
	Name        string `json:"name" yaml:"name" mapstructure:"name"`
	Concurrency int    `json:"concurrency" yaml:"concurrency" mapstructure:"concurrency"`
}

type KafkaTLSConfig struct {
	Enabled bool `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
}

type KafkaSASLConfig struct {
	Enabled   bool   `json:"enabled" yaml:"enabled" mapstructure:"enabled"`
	Mechanism string `json:"mechanism" yaml:"mechanism" mapstructure:"mechanism"`
	Username  string `json:"username" yaml:"username" mapstructure:"username"`
	Password  string `json:"password" yaml:"password" mapstructure:"password"`
}

func NewManager(config *WmKafkaConfig) *WmKafkaManager {
	wm := &WmKafkaManager{
		config: config,
	}
	wm.subscriber = initSubscriber(wm.config)
	wm.publisher = initPublisher(wm.config)

	return wm
}

func NewManagerWithConfigKey(key string) (*WmKafkaManager, error) {
	cfg, err := ParseConfig(key)
	if err != nil {
		return nil, err
	}
	return NewManager(cfg), nil
}

func ParseConfig(key string) (*WmKafkaConfig, error) {
	if !viper.IsSet(key) {
		return nil, fmt.Errorf("key %s not configured", key)
	}

	var cfg WmKafkaConfig
	if err := viper.UnmarshalKey(key, &cfg); err != nil {
		return nil, fmt.Errorf("parse config key: %s, error: %v", key, err)
	}

	return &cfg, nil
}

// 消费消息（自动重连）
func (m *WmKafkaManager) Consume(ctx context.Context, topic string, concurrency int, handleMessage func(context.Context, string) error) error {
	return consume(ctx, topic, concurrency, m.subscriber, handleMessage)
}

// 发布消息
func (m *WmKafkaManager) Publish(topic string, msgData any) error {
	return publishMsgs(m.publisher, topic, []any{msgData})
}

// 批量发布消息
func (m *WmKafkaManager) PublishMsgs(topic string, msgDatas []any) error {
	return publishMsgs(m.publisher, topic, msgDatas)
}

// 关闭
func (m *WmKafkaManager) Close() {
	if m.subscriber != nil {
		m.subscriber.Close()
	}
	if m.publisher != nil {
		m.publisher.Close()
	}
}
