package redismq

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/joepeak/golib-conf"

	"github.com/hibiken/asynq"
	redis "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

var (
	RedisClient redis.UniversalClient
)

func init() {
	if !viper.IsSet("mq.redismq") {
		return
	}

	if client, err := NewRedisClient(); err != nil {
		logrus.Fatal("Failed to initialize Redis client: ", err)
	} else {
		RedisClient = client
	}
}

// ===== Redis 连接管理 =====

// getAsynqRedisConnOpt 获取 asynq Redis 连接配置
func getAsynqRedisConnOpt() asynq.RedisConnOpt {
	if viper.GetBool("mq.redismq.redis.enabledCluster") {
		addrs := viper.GetStringSlice("mq.redismq.redis.cluster.addrs")
		return asynq.RedisClusterClientOpt{
			Addrs:    addrs,
			Password: viper.GetString("mq.redismq.redis.password"),
		}
	}

	return asynq.RedisClientOpt{
		Addr:     viper.GetString("mq.redismq.redis.addr"),
		Password: viper.GetString("mq.redismq.redis.password"),
		DB:       viper.GetInt("mq.redismq.redis.db"),
	}
}

// NewRedisClient 初始化 Redis 连接
func NewRedisClient() (redis.UniversalClient, error) {
	if viper.GetBool("mq.redismq.redis.enabledCluster") {
		return newClusterClient()
	}
	return newSingleClient()
}

// 初始化单机模式 Redis
func newSingleClient() (redis.UniversalClient, error) {
	opts := &redis.Options{
		Addr:      viper.GetString("mq.redismq.redis.addr"),
		Password:  viper.GetString("mq.redismq.redis.password"),
		DB:        viper.GetInt("mq.redismq.redis.db"),
		TLSConfig: getTLSConfig(),
	}

	client := redis.NewClient(opts)

	if err := pingRedis(client); err != nil {
		logrus.Error("Failed to connect to Redis: ", err, ", addr: ", opts.Addr)
		return nil, err
	}

	logrus.Info("Connected to RedisMQ successfully, addr: ", opts.Addr, ", db: ", opts.DB)
	return client, nil
}

// 初始化 Redis Cluster 模式
func newClusterClient() (redis.UniversalClient, error) {
	addrs := viper.GetStringSlice("mq.redismq.redis.cluster.addrs")
	opts := &redis.ClusterOptions{
		Addrs:     addrs,
		Password:  viper.GetString("mq.redismq.redis.password"),
		TLSConfig: getTLSConfig(),
	}

	client := redis.NewClusterClient(opts)

	if err := pingRedis(client); err != nil {
		logrus.Error("Failed to connect to Redis Cluster: ", err, ", addrs: ", strings.Join(addrs, ","))
		return nil, err
	}

	logrus.Info("Connected to RedisMQ Cluster successfully, addrs: ", strings.Join(addrs, ","))
	return client, nil
}

// TLS 配置
func getTLSConfig() *tls.Config {
	if viper.GetBool("mq.redismq.redis.enabledTls") {
		return &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	return nil
}

// Redis 连接测试
func pingRedis(client redis.UniversalClient) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	return client.Ping(ctx).Err()
}

// ===== 任务管理 =====

// NewTask 创建任务
func NewTask(name string, data any) (*asynq.Task, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("new task, json marshal error, %s", err.Error())
	}

	return asynq.NewTask(name, payload), nil
}

// ===== 管理器 =====

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

		// 获取 asynq Redis 连接配置
		redisConnOpt := getAsynqRedisConnOpt()

		manager := &RedisMQManager{
			client:   asynq.NewClient(redisConnOpt),
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

	m.server = asynq.NewServer(getAsynqRedisConnOpt(), asynq.Config{
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

// ===== 全局便捷函数 =====

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
