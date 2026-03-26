package wmkafka

import (
	"context"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/joepeak/golib-mq/util"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	logPrefix = "[wmkafka]"

	defaultSubscriber *kafka.Subscriber
	defaultPublisher  *kafka.Publisher
)

// 初始化 Kafka 连接
func init() {
	if !viper.IsSet("mq.kafkamq") || !viper.IsSet("mq.watermill") {
		return
	}

	var err error
	defaultConfig, err := ParseConfig("mq")
	if err != nil {
		logrus.Fatal(logPrefix, "parse config error, ", err)
	}

	if len(defaultConfig.Kafka.Brokers) == 0 {
		logrus.Fatal(logPrefix, "mq.kafkamq.brokers not set")
		return
	}

	defaultSubscriber = initSubscriber(defaultConfig)
	defaultPublisher = initPublisher(defaultConfig)
}

// 初始化 Kafka 订阅者
func initSubscriber(cfg *WmKafkaConfig) *kafka.Subscriber {
	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // 默认消费最新消息
	saramaConfig.Net.SASL.Enable = cfg.Kafka.SASL.Enabled
	saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.Kafka.SASL.Mechanism)
	saramaConfig.Net.SASL.User = cfg.Kafka.SASL.Username
	saramaConfig.Net.SASL.Password = cfg.Kafka.SASL.Password
	saramaConfig.Net.TLS.Enable = cfg.Kafka.TLS.Enabled // 如果 Kafka 需要 TLS 连接
	saramaConfig.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRange(),
	}

	subscriber, err := kafka.NewSubscriber(
		kafka.SubscriberConfig{
			Brokers:               cfg.Kafka.Brokers,
			Unmarshaler:           kafka.DefaultMarshaler{},
			ConsumerGroup:         cfg.Kafka.Consumer.Name,
			OverwriteSaramaConfig: saramaConfig,
		},
		watermill.NewStdLogger(cfg.Watermill.Logger.Debug, cfg.Watermill.Logger.Trace),
	)

	if err != nil {
		logrus.Fatal(logPrefix, "Failed to create Kafka subscriber: ", err)
	}

	logrus.Info(logPrefix, "Kafka subscriber initialized: ", cfg.Kafka.Consumer.Name, " brokers: ", cfg.Kafka.Brokers)
	return subscriber
}

// 初始化 Kafka 生产者
func initPublisher(cfg *WmKafkaConfig) *kafka.Publisher {
	saramaConfig := kafka.DefaultSaramaSyncPublisherConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // 默认消费最新消息
	saramaConfig.Net.SASL.Enable = cfg.Kafka.SASL.Enabled
	saramaConfig.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.Kafka.SASL.Mechanism)
	saramaConfig.Net.SASL.User = cfg.Kafka.SASL.Username
	saramaConfig.Net.SASL.Password = cfg.Kafka.SASL.Password
	saramaConfig.Net.TLS.Enable = cfg.Kafka.TLS.Enabled // 如果 Kafka 需要 TLS 连接

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               cfg.Kafka.Brokers,
			Marshaler:             kafka.DefaultMarshaler{},
			OverwriteSaramaConfig: saramaConfig,
		},
		watermill.NewStdLogger(cfg.Watermill.Logger.Debug, cfg.Watermill.Logger.Trace),
	)

	if err != nil {
		logrus.Fatal(logPrefix, "Failed to create Kafka publisher: ", err)
	}

	logrus.Info(logPrefix, "Kafka publisher initialized, brokers: ", cfg.Kafka.Brokers)

	return publisher
}

// 消费消息（自动重连）
func Consume(ctx context.Context, topic string, concurrency int,
	handler func(context.Context, string) error) error {
	if defaultSubscriber == nil {
		return fmt.Errorf("default subscriber not init, topic: %s", topic)
	}

	return consume(ctx, topic, concurrency, defaultSubscriber, handler)
}

// 消费消息（自动重连）
func consume(ctx context.Context, topic string, concurrency int, subscriber *kafka.Subscriber,
	handler func(context.Context, string) error) error {
	messages, err := subscriber.Subscribe(ctx, topic)
	if err != nil {
		logrus.Error(logPrefix, "Kafka subscribe error, topic: ", topic, ", error: ", err)
		return err
	}

	for i := 1; i <= concurrency; i++ {
		go func(workerID int) {
			logrus.Info(logPrefix, "Kafka consumer for topic: ", topic, " workerID: ", workerID)
			for {
				select {
				case <-ctx.Done():
					logrus.Info(logPrefix, "Stopping Kafka consumer for topic: ", topic)
					return
				case msg, ok := <-messages:
					if !ok {
						logrus.Warn(logPrefix, "Kafka message close, stop consume")
						return
					}

					msgData := string(msg.Payload)
					logrus.Debug(logPrefix, "-[", topic, "-work-", workerID, "], Received message: ", msg.UUID, ", msgData: ", msgData)

					if err := handler(ctx, msgData); err != nil {
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
func publishMsgs(publisher *kafka.Publisher, topic string, msgDatas []any) error {
	if publisher == nil {
		return fmt.Errorf("publisher not init")
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
