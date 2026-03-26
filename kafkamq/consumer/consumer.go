package consumer

import (
	"log"

	"github.com/IBM/sarama"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	consumer sarama.Consumer
	err      error
)

func init() {

	if !viper.IsSet("mq.kafkamq") {
		return
	}

	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	addrs := viper.GetStringSlice("mq.kafkamq.addrs")
	consumer, err = sarama.NewConsumer(addrs, config)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}

	logrus.Info("kafka consumer init success, addrs: ", addrs)
}

// 关闭消费者
func Close() {
	if consumer != nil {
		consumer.Close()
	}
}

// 消费消息
func ConsumeMessage(topic string) (sarama.PartitionConsumer, error) {
	return ConsumeMessageWithPartition(topic, 0, sarama.OffsetNewest)
}

// 消费消息
func ConsumeMessageWithOffset(topic string, offset int64) (sarama.PartitionConsumer, error) {
	return ConsumeMessageWithPartition(topic, 0, offset)
}

// 消费消息
func ConsumeMessageWithPartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return consumer.ConsumePartition(topic, partition, offset)
}
