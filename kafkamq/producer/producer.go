package producer

import (
	"log"

	"github.com/IBM/sarama"
	_ "github.com/joepeak/golib-conf"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	producer      sarama.SyncProducer
	asyncProducer sarama.AsyncProducer
)

func init() {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	addrs := viper.GetStringSlice("mq.kafkamq.addrs")
	logrus.Info("kafka producer init success, addrs: ", addrs)

	var err error

	producer, err = sarama.NewSyncProducer(addrs, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	asyncProducer, err = sarama.NewAsyncProducer(addrs, config)
	if err != nil {
		log.Fatalf("Failed to create async producer: %v", err)
	}
}

// 发送消息
func SendMessage(topic string, key string, value string) error {
	return SendMessageWithPartition(topic, 0, key, value)
}

// 发送消息到分区
func SendMessageWithPartition(topic string, partition int32, key string, value string) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.StringEncoder(value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return err
	}

	logrus.Debug("Send message success, topic: ", msg.Topic, ", partition: ", partition, ", key: ", key, ", value: ", value, ", offset: ", offset)

	return nil
}

// 异步发送消息
func AsyncSendMessage(topic string, key string, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}

	asyncProducer.Input() <- msg

	return nil
}

// 关闭发送者
func Close() {
	if producer != nil {
		producer.Close()
	}

	if asyncProducer != nil {
		asyncProducer.Close()
	}
}
