package consumer

import (
	"context"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"

	_ "github.com/joepeak/golib-conf"
	"github.com/joepeak/golib-mq/redismq"
)

var (
	srv *asynq.Server
	mux *asynq.ServeMux

	defaultQueues = map[string]int{
		"critical": 6,
		"default":  3,
		"low":      1,
	}
)

func init() {
	concurrency := viper.GetInt("mq.redismq.consumer.concurrency")

	// 定义队列结构
	type Queue struct {
		Name     string `mapstructure:"name"`
		Priority int    `mapstructure:"priority"`
	}

	// 初始化消费队列
	consumerQueues := defaultQueues

	// 解析 viper 配置
	var queues []Queue
	if err := viper.UnmarshalKey("mq.redismq.consumer.queues", &queues); err != nil {
		logrus.Fatal("Failed to parse consumer queues, error: ", err)
	} else if len(queues) > 0 {
		consumerQueues = make(map[string]int, len(queues))
		for _, v := range queues {
			consumerQueues[v.Name] = v.Priority
		}
	}

	logrus.Info("Consumer initialized, queues: ", consumerQueues)

	// **增加 Redis 连接检测**
	if err := redismq.RedisClient.Ping(context.Background()).Err(); err != nil {
		logrus.Fatal("Failed to connect to Redis for Asynq, error: ", err)
	}

	srv = asynq.NewServerFromRedisClient(redismq.RedisClient, asynq.Config{
		Concurrency: concurrency,
		Queues:      consumerQueues,
	})
}

// 运行消费者
func Run(ctx context.Context, handlers map[string]func(ctx context.Context, task *asynq.Task) error) error {
	mux = asynq.NewServeMux()

	for taskType, handler := range handlers {
		mux.HandleFunc(taskType, handler)
		logrus.Info("Registered consumer, task type: ", taskType)
	}

	// 监听 context 取消
	go func() {
		<-ctx.Done()
		logrus.Info("Context cancelled, shutting down consumer...")
		srv.Shutdown()
	}()

	return srv.Run(mux)
}
