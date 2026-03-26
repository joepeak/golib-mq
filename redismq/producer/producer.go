package producer

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hibiken/asynq"

	_ "github.com/joepeak/golib-conf"
	"github.com/joepeak/golib-mq/redismq"
)

var (
	client *asynq.Client
)

func init() {
	client = asynq.NewClientFromRedisClient(redismq.RedisClient)
}

// 推送消息
func Push(taskName string, data any) (*asynq.TaskInfo, error) {
	task, err := NewTask(taskName, data)
	if err != nil {
		return nil, err
	}

	return Enqueue(task)
}

func PushInQueue(queueName, taskName string, data any, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	task, err := NewTask(taskName, data)
	if err != nil {
		return nil, err
	}

	return Enqueue(task, append(opts, asynq.Queue(queueName))...)
}

// 推送消息并延迟
func PushWithDelay(queueName, taskName string, data any, duration time.Duration) (*asynq.TaskInfo, error) {
	task, err := NewTask(taskName, data)
	if err != nil {
		return nil, err
	}

	return Enqueue(task, asynq.ProcessIn(duration), asynq.Queue(queueName))
}

// 推送消息并指定时间
func PushWithTime(taskName string, data any, t time.Time) (*asynq.TaskInfo, error) {
	task, err := NewTask(taskName, data)
	if err != nil {
		return nil, err
	}

	return Enqueue(task, asynq.ProcessAt(t))
}

// NewTask 创建任务
func NewTask(name string, data any) (*asynq.Task, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("new task, json marshal error, %s", err.Error())
	}

	return asynq.NewTask(name, payload), nil
}

// Enqueue 入队任务
func Enqueue(task *asynq.Task, opts ...asynq.Option) (*asynq.TaskInfo, error) {
	if info, err := client.Enqueue(task, opts...); err != nil {
		return nil, fmt.Errorf("enqueue task error, %s", err.Error())
	} else {
		return info, nil
	}
}
