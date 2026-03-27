package redismq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// RedisMQManager Redis MQ管理器
type RedisMQManager struct {
	client   *asynq.Client
	server   *asynq.Server
	mux      *asynq.ServeMux
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	handlers map[string]asynq.HandlerFunc
	mu       sync.RWMutex
	running  bool
}

var globalManager *RedisMQManager
var managerOnce sync.Once

// GetManager 获取全局管理器（单例）
func GetManager() *RedisMQManager {
	managerOnce.Do(func() {
		if RedisClient == nil {
			logrus.Fatal("Redis client not initialized, please check configuration")
			return
		}

		ctx, cancel := context.WithCancel(context.Background())
		manager := &RedisMQManager{
			client:   asynq.NewClientFromRedisClient(RedisClient),
			handlers: make(map[string]asynq.HandlerFunc),
			ctx:      ctx,
			cancel:   cancel,
		}

		globalManager = manager
		logrus.Info("RedisMQ manager initialized")
	})

	return globalManager
}

// RegisterHandler 注册任务处理器
func (m *RedisMQManager) RegisterHandler(taskType string, handler asynq.HandlerFunc) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.mux == nil {
		m.mux = asynq.NewServeMux()
	}
	
	m.mux.HandleFunc(taskType, handler)
	m.handlers[taskType] = handler
	logrus.Infof("Registered handler for task type: %s, total handlers: %d", taskType, len(m.handlers))
	return nil
}

// EnqueueTask 入队任务
func (m *RedisMQManager) EnqueueTask(taskType string, payload interface{}, options ...asynq.Option) (*asynq.TaskInfo, error) {
	if m == nil || m.client == nil {
		return nil, fmt.Errorf("RedisMQ manager not initialized")
	}

	task, err := NewTask(taskType, payload)
	if err != nil {
		return nil, err
	}

	return m.client.Enqueue(task, options...)
}

// EnqueueTaskWithDelay 延迟入队任务
func (m *RedisMQManager) EnqueueTaskWithDelay(taskType string, payload interface{}, delay time.Duration) (*asynq.TaskInfo, error) {
	return m.EnqueueTask(taskType, payload, asynq.ProcessIn(delay))
}

// EnqueueTaskAt 指定时间入队任务
func (m *RedisMQManager) EnqueueTaskAt(taskType string, payload interface{}, processAt time.Time) (*asynq.TaskInfo, error) {
	return m.EnqueueTask(taskType, payload, asynq.ProcessAt(processAt))
}

// EnqueueCriticalTask 入队高优先级任务
func (m *RedisMQManager) EnqueueCriticalTask(taskType string, payload interface{}) (*asynq.TaskInfo, error) {
	return m.EnqueueTask(taskType, payload, asynq.Queue("critical"))
}

// EnqueueLowTask 入队低优先级任务
func (m *RedisMQManager) EnqueueLowTask(taskType string, payload interface{}) (*asynq.TaskInfo, error) {
	return m.EnqueueTask(taskType, payload, asynq.Queue("low"))
}

// Start 启动服务器
func (m *RedisMQManager) Start(concurrency int, queues map[string]int) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.running {
		return fmt.Errorf("RedisMQ server already running")
	}

	if m.mux == nil {
		m.mux = asynq.NewServeMux()
	}

	// 使用默认队列配置
	if queues == nil {
		queues = map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		}
	}

	m.server = asynq.NewServerFromRedisClient(RedisClient, asynq.Config{
		Concurrency: concurrency,
		Queues:      queues,
	})

	m.running = true
	m.wg.Add(1)

	go func() {
		defer m.wg.Done()
		defer func() {
			m.running = false
		}()

		logrus.Info("Starting RedisMQ server...")
		if err := m.server.Run(m.mux); err != nil {
			logrus.Errorf("RedisMQ server error: %v", err)
		}
	}()

	logrus.Info("RedisMQ server started successfully")
	return nil
}

// Stop 停止服务器
func (m *RedisMQManager) Stop() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.running {
		return
	}

	logrus.Info("Stopping RedisMQ server...")

	// 取消上下文
	m.cancel()

	// 关闭服务器
	if m.server != nil {
		m.server.Shutdown()
	}

	// 关闭客户端
	if m.client != nil {
		m.client.Close()
	}

	// 等待goroutine完成
	m.wg.Wait()

	m.running = false
	logrus.Info("RedisMQ server stopped")
}

// IsRunning 检查服务器是否运行
func (m *RedisMQManager) IsRunning() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.running
}

// GetStats 获取统计信息
func (m *RedisMQManager) GetStats() map[string]interface{} {
	if m == nil {
		return map[string]interface{}{
			"available": false,
			"message":   "RedisMQ manager not initialized",
		}
	}

	stats := map[string]interface{}{
		"available":    true,
		"running":      m.IsRunning(),
		"handlers":     len(m.handlers),
		"handlerTypes": m.getHandlerTypes(),
	}

	return stats
}

// getHandlerTypes 获取处理器类型列表
func (m *RedisMQManager) getHandlerTypes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	types := make([]string, 0, len(m.handlers))
	for taskType := range m.handlers {
		types = append(types, taskType)
	}
	return types
}
