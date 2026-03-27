package redismq

import (
	"fmt"
	"time"

	"github.com/hibiken/asynq"
)

// 全局便捷函数

// EnqueueTask 全局入队任务函数
func EnqueueTask(taskType string, payload interface{}, options ...asynq.Option) (*asynq.TaskInfo, error) {
	manager := GetManager()
	if manager == nil {
		return nil, fmt.Errorf("RedisMQ manager not available")
	}
	return manager.EnqueueTask(taskType, payload, options...)
}

// EnqueueTaskWithDelay 全局延迟入队任务函数
func EnqueueTaskWithDelay(taskType string, payload interface{}, delay time.Duration) (*asynq.TaskInfo, error) {
	manager := GetManager()
	if manager == nil {
		return nil, fmt.Errorf("RedisMQ manager not available")
	}
	return manager.EnqueueTaskWithDelay(taskType, payload, delay)
}

// EnqueueTaskAt 全局指定时间入队任务函数
func EnqueueTaskAt(taskType string, payload interface{}, processAt time.Time) (*asynq.TaskInfo, error) {
	manager := GetManager()
	if manager == nil {
		return nil, fmt.Errorf("RedisMQ manager not available")
	}
	return manager.EnqueueTaskAt(taskType, payload, processAt)
}

// EnqueueCriticalTask 全局高优先级入队任务函数
func EnqueueCriticalTask(taskType string, payload interface{}) (*asynq.TaskInfo, error) {
	manager := GetManager()
	if manager == nil {
		return nil, fmt.Errorf("RedisMQ manager not available")
	}
	return manager.EnqueueCriticalTask(taskType, payload)
}

// EnqueueLowTask 全局低优先级入队任务函数
func EnqueueLowTask(taskType string, payload interface{}) (*asynq.TaskInfo, error) {
	manager := GetManager()
	if manager == nil {
		return nil, fmt.Errorf("RedisMQ manager not available")
	}
	return manager.EnqueueLowTask(taskType, payload)
}

// RegisterTaskHandler 全局注册任务处理器函数
func RegisterTaskHandler(taskType string, handler asynq.HandlerFunc) error {
	manager := GetManager()
	if manager == nil {
		return fmt.Errorf("RedisMQ manager not available")
	}
	return manager.RegisterHandler(taskType, handler)
}

// StartRedisMQServer 启动全局RedisMQ服务器
func StartRedisMQServer(concurrency int, queues map[string]int) error {
	manager := GetManager()
	if manager == nil {
		return fmt.Errorf("RedisMQ manager not available")
	}
	return manager.Start(concurrency, queues)
}

// StopRedisMQServer 停止全局RedisMQ服务器
func StopRedisMQServer() {
	manager := GetManager()
	if manager != nil {
		manager.Stop()
	}
}

// GetRedisMQStats 获取RedisMQ统计信息
func GetRedisMQStats() map[string]interface{} {
	manager := GetManager()
	if manager == nil {
		return map[string]interface{}{
			"available": false,
			"message":   "RedisMQ manager not available",
		}
	}
	return manager.GetStats()
}
