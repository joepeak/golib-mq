package wmnats

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-nats/v2/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/joepeak/golib-mq/util"
	natsgo "github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

const (
	logPrefix = "[wmnats]"

	ConfigKeyMQ        = "mq"
	ConfigKeyWatermill = "mq.watermill"
	ConfigKeyNatsMQ    = "mq.natsmq"
)

var (
	defaultSubscriber *nats.Subscriber
	defaultPublisher  *nats.Publisher
)

// 初始化连接
func init() {
	if !viper.IsSet(ConfigKeyWatermill) || !viper.IsSet(ConfigKeyNatsMQ) {
		return
	}

	var err error
	defaultConfig, err := ParseConfig(ConfigKeyMQ)
	if err != nil {
		logrus.Fatal(logPrefix, "parse config error, ", err)
	}

	if len(defaultConfig.Nats.URL) == 0 {
		logrus.Fatal(logPrefix, "Nats url not set")
		return
	}

	defaultSubscriber = initSubscriber(defaultConfig)
	defaultPublisher = initPublisher(defaultConfig)
}

// 初始化订阅者
func initSubscriber(cfg *WmNatsConfig) *nats.Subscriber {

	// nats options
	options := []natsgo.Option{
		natsgo.RetryOnFailedConnect(cfg.Nats.RetryConnect),
		natsgo.Timeout(cfg.Nats.Timeout * time.Second),
		natsgo.ReconnectWait(cfg.Nats.Timeout * time.Second),
	}

	// sub options
	subOpts := []natsgo.SubOpt{
		natsgo.DeliverAll(),
		natsgo.AckExplicit(),
		natsgo.Durable(cfg.Nats.Consumer.Name),
	}

	// jet stream config
	jsConfig := nats.JetStreamConfig{
		Disabled:         false,
		AutoProvision:    true,
		ConnectOptions:   nil,
		SubscribeOptions: subOpts,
		PublishOptions:   nil,
		TrackMsgId:       false,
		AckAsync:         false,
		DurablePrefix:    cfg.Nats.Consumer.Name,
	}

	// logger
	logger := watermill.NewStdLogger(cfg.Watermill.Logger.Debug, cfg.Watermill.Logger.Trace)

	// new subscriber
	subscriber, err := nats.NewSubscriber(
		nats.SubscriberConfig{
			URL:            cfg.Nats.URL,
			CloseTimeout:   cfg.Nats.Timeout * time.Second,
			AckWaitTimeout: cfg.Nats.Timeout * time.Second,
			NatsOptions:    options,
			Unmarshaler:    &nats.GobMarshaler{},
			JetStream:      jsConfig,
		},
		logger,
	)

	if err != nil {
		logrus.WithFields(logrus.Fields{
			"natsURL": cfg.Nats.URL,
		}).Fatal(logPrefix, "Failed to create nats subscriber: ", err)
	}

	logrus.WithFields(logrus.Fields{
		"natsURL": cfg.Nats.URL,
	}).Info(logPrefix, "Subscriber initialized: ", cfg.Nats.Consumer.Name)

	return subscriber
}

// 初始化生产者
func initPublisher(cfg *WmNatsConfig) *nats.Publisher {
	// nats options
	options := []natsgo.Option{
		natsgo.RetryOnFailedConnect(cfg.Nats.RetryConnect),
		natsgo.Timeout(cfg.Nats.Timeout * time.Second),
		natsgo.ReconnectWait(cfg.Nats.Timeout * time.Second),
	}

	// sub options (for publisher, these are used for internal subscriptions if needed)
	subOpts := []natsgo.SubOpt{
		natsgo.DeliverAll(),
		natsgo.AckExplicit(),
	}

	// jet stream config
	jsConfig := nats.JetStreamConfig{
		Disabled:         false,
		AutoProvision:    true,
		ConnectOptions:   nil,
		SubscribeOptions: subOpts,
		PublishOptions:   nil,
		TrackMsgId:       false,
		AckAsync:         false,
		DurablePrefix:    cfg.Nats.Consumer.Name,
	}

	logger := watermill.NewStdLogger(cfg.Watermill.Logger.Debug, cfg.Watermill.Logger.Trace)

	publisher, err := nats.NewPublisher(
		nats.PublisherConfig{
			URL:         cfg.Nats.URL,
			NatsOptions: options,
			Marshaler:   &nats.GobMarshaler{},
			JetStream:   jsConfig,
		},
		logger,
	)

	if err != nil {
		logrus.Fatal(logPrefix, "Failed to create publisher: ", err)
	}

	logrus.Info(logPrefix, "Publisher initialized: ", cfg.Nats.Consumer.Name,
		", URL: ", cfg.Nats.URL)

	return publisher
}

// 消费消息（自动重连）
func Consume(ctx context.Context, topic string, concurrency int,
	handle func(context.Context, string) error) error {
	if defaultSubscriber == nil {
		return fmt.Errorf("default subscriber not init, topic: %s", topic)
	}

	return consume(ctx, topic, concurrency, defaultSubscriber, handle)
}

// 消费消息（自动重连）
func consume(ctx context.Context, topic string, concurrency int, subscriber *nats.Subscriber,
	handle func(context.Context, string) error) error {
	messages, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		logrus.Error(logPrefix, "Subscribe error, topic: ", topic, ", error: ", err)
		return err
	}

	for i := 1; i <= concurrency; i++ {
		go func(workerID int) {
			logrus.Info(logPrefix, "Consumer for topic: ", topic, " workerID: ", workerID)
			for {
				select {
				case <-ctx.Done():
					logrus.Info(logPrefix, "Stopping consumer for topic: ", topic)
					return
				case msg, ok := <-messages:
					if !ok {
						logrus.Warn(logPrefix, "Message close, stop consume")
						return
					}

					msgData := string(msg.Payload)

					logrus.WithFields(logrus.Fields{
						"topic":    topic,
						"workerID": workerID,
						"msgID":    msg.UUID,
						"msgData":  msgData,
					}).Debug(logPrefix, "Received message")

					if err := handle(ctx, msgData); err != nil {
						msg.Nack()
						continue
					}
					msg.Ack()
				}
			}
		}(i)
	}

	return nil
}

// 发布消息
func Publish(topic string, msgData any) error {
	return publishMsgs(defaultPublisher, topic, []any{msgData})
}

func PublishMsgs(topic string, msgDatas []any) error {
	return publishMsgs(defaultPublisher, topic, msgDatas)
}

// 批量发布消息
func publishMsgs(publisher *nats.Publisher, topic string, msgDatas []any) error {
	if publisher == nil {
		return fmt.Errorf("publisher not init, topic: %s", topic)
	}

	var msgs []*message.Message

	for _, msgData := range msgDatas {
		msg := message.NewMessage(watermill.NewUUID(), []byte(util.ObjToJson(msgData)))
		msgs = append(msgs, msg)
	}

	return publisher.Publish(topic, msgs...)
}

func Close() {
	if defaultPublisher != nil {
		defaultPublisher.Close()
	}

	if defaultSubscriber != nil {
		defaultSubscriber.Close()
	}
}
