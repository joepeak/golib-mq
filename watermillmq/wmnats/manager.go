package wmnats

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/spf13/viper"
)

type WmNatsManager struct {
	subscriber *nats.Subscriber
	publisher  *nats.Publisher
	config     *WmNatsConfig
}

type WmNatsConfig struct {
	Watermill WatermillConfig `json:"watermill" yaml:"watermill" mapstructure:"watermill"`
	Nats      NatsConfig      `json:"natsmq" yaml:"natsmq" mapstructure:"natsmq"`
}

type WatermillConfig struct {
	Logger LoggerConfig `json:"logger" yaml:"logger" mapstructure:"logger"`
}

type LoggerConfig struct {
	Debug bool `json:"debug" yaml:"debug" mapstructure:"debug"`
	Trace bool `json:"trace" yaml:"trace" mapstructure:"trace"`
}

type NatsConfig struct {
	Consumer       ConsumerConfig `json:"consumer" yaml:"consumer" mapstructure:"consumer"`
	URL            string         `json:"url" yaml:"url" mapstructure:"url"`
	RetryConnect   bool           `json:"retryConnect" yaml:"retryConnect" mapstructure:"retryConnect"`
	Timeout        time.Duration  `json:"timeout" yaml:"timeout" mapstructure:"timeout"`
	ReconnectDelay int32          `json:"reconnectDelay" yaml:"reconnectDelay" mapstructure:"reconnectDelay"`
}

type ConsumerConfig struct {
	Name        string `json:"name" yaml:"name" mapstructure:"name"`
	Concurrency int    `json:"concurrency" yaml:"concurrency" mapstructure:"concurrency"`
}

func NewManager(config *WmNatsConfig) *WmNatsManager {
	wm := &WmNatsManager{
		config: config,
	}
	wm.subscriber = initSubscriber(wm.config)
	wm.publisher = initPublisher(wm.config)

	return wm
}

func NewManagerWithConfigKey(key string) (*WmNatsManager, error) {
	cfg, err := ParseConfig(key)
	if err != nil {
		return nil, err
	}
	return NewManager(cfg), nil
}

func ParseConfig(key string) (*WmNatsConfig, error) {
	if !viper.IsSet(key) {
		return nil, fmt.Errorf("key %s not configured", key)
	}

	var cfg WmNatsConfig
	if err := viper.UnmarshalKey(key, &cfg); err != nil {
		return nil, fmt.Errorf("parse config key: %s, error: %v", key, err)
	}

	return &cfg, nil
}

// 消费消息（自动重连）
func (m *WmNatsManager) Consume(ctx context.Context, topic string, concurrency int,
	handle func(context.Context, string) error) error {
	return consume(ctx, topic, concurrency, m.subscriber, handle)
}

// 发布消息
func (m *WmNatsManager) Publish(topic string, msgData any) error {
	return publishMsgs(m.publisher, topic, []any{msgData})
}

// 批量发布消息
func (m *WmNatsManager) PublishMsgs(topic string, msgDatas []any) error {
	return publishMsgs(m.publisher, topic, msgDatas)
}

// 关闭
func (m *WmNatsManager) Close() {
	if m.subscriber != nil {
		m.subscriber.Close()
	}
	if m.publisher != nil {
		m.publisher.Close()
	}
}
