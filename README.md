# Golib MQ

Go 语言消息队列库，支持 Kafka、Redis、NATS 等多种消息队列，提供统一的消息处理接口。

## 功能特性

- 🚀 **多队列支持**: 支持 Kafka、Redis、NATS、Watermill 等主流消息队列
- 🔄 **统一接口**: 提供统一的 Producer 和 Consumer 接口，便于切换
- ⚡ **高性能**: 基于官方客户端，性能优化
- 🛡️ **错误处理**: 完善的错误处理和重连机制
- 📊 **监控集成**: 内置日志和监控支持
- 🔧 **灵活配置**: 支持多种配置方式和连接参数
- 🔒 **分布式锁**: 集成 Redis 分布式锁功能

## 支持的消息队列

| 队列类型 | 包路径 | 特性 | 适用场景 |
|----------|--------|------|----------|
| **Kafka** | `github.com/joepeak/golib-mq/kafkamq` | 高吞吐量，分区机制 | 大数据量，日志收集 |
| **Redis MQ** | `github.com/joepeak/golib-mq/redismq` | 轻量级，持久化，任务队列 | 异步任务，可靠消息传递 |
| **Redis Client** | `github.com/joepeak/golib-mq/redisclient` | Pub/Sub，分布式锁 | 实时通知，分布式同步 |
| **Watermill-Kafka** | `github.com/joepeak/golib-mq/watermillmq/wmkafka` | 事件驱动，流处理 | 事件溯源，CQRS |
| **Watermill-NATS** | `github.com/joepeak/golib-mq/watermillmq/wmnats` | 云原生，微服务 | 微服务通信 |

## 快速开始

### 安装

```bash
# 安装完整库
go get github.com/joepeak/golib-mq

# 或通过元包安装
go get github.com/joepeak/golib-toolbox
```

### Kafka 示例

```go
package main

import (
    "context"
    "log"
    
    "github.com/joepeak/golib-mq/kafkamq"
    _ "github.com/joepeak/golib-conf"  // 配置初始化
)

func main() {
    // 创建生产者
    producer, err := kafkamq.NewProducer()
    if err != nil {
        log.Fatal(err)
    }
    
    // 发送消息
    err = producer.SendMessage("test-topic", "Hello Kafka!")
    if err != nil {
        log.Printf("发送失败: %v", err)
    }
    
    // 创建消费者
    consumer, err := kafkamq.NewConsumer("test-group")
    if err != nil {
        log.Fatal(err)
    }
    
    // 订阅消息
    consumer.Subscribe("test-topic", func(message []byte) error {
        log.Printf("收到消息: %s", string(message))
        return nil
    })
}
```

### Redis MQ 示例 (基于Asynq)

#### 方式1：使用全局函数（推荐）

```go
package main

import (
    "context"
    "log"
    
    "github.com/joepeak/golib-mq/redismq"
    "github.com/hibiken/asynq"
    _ "github.com/joepeak/golib-conf"
)

func main() {
    // 注册任务处理器
    err := redismq.RegisterTaskHandler("email:send", func(ctx context.Context, task *asynq.Task) error {
        log.Printf("发送邮件: %s", string(task.Payload()))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动服务器
    err = redismq.StartRedisMQServer(10, map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 发送任务
    taskInfo, err := redismq.EnqueueTask("email:send", map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Hello",
        "body":    "Test message",
    })
    if err != nil {
        log.Printf("发送失败: %v", err)
    } else {
        log.Printf("任务ID: %s", taskInfo.ID)
    }
}
```

#### 方式2：使用管理器对象

```go
package main

import (
    "context"
    "log"
    
    "github.com/joepeak/golib-mq/redismq"
    "github.com/hibiken/asynq"
    _ "github.com/joepeak/golib-conf"
)

func main() {
    // 获取管理器
    manager := redismq.GetManager()
    
    // 注册处理器
    err := manager.RegisterHandler("email:send", func(ctx context.Context, task *asynq.Task) error {
        log.Printf("发送邮件: %s", string(task.Payload()))
        return nil
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 启动服务器
    err = manager.Start(10, map[string]int{
        "critical": 6,
        "default":  3,
        "low":      1,
    })
    if err != nil {
        log.Fatal(err)
    }
    
    // 发送任务
    taskInfo, err := manager.EnqueueTask("email:send", map[string]interface{}{
        "to":      "user@example.com",
        "subject": "Hello",
        "body":    "Test message",
    })
    if err != nil {
        log.Printf("发送失败: %v", err)
    } else {
        log.Printf("任务ID: %s", taskInfo.ID)
    }
    
    // 获取统计信息
    stats := manager.GetStats()
    log.Printf("RedisMQ状态: %+v", stats)
}
```

### Redis Client 示例 (Pub/Sub + 分布式锁)

```go
package main

import (
    "context"
    "log"
    "time"
    
    "github.com/joepeak/golib-mq/redisclient"
    _ "github.com/joepeak/golib-conf"
)

func main() {
    // Pub/Sub 发布消息
    err := redisclient.Publish(context.Background(), "notifications", map[string]interface{}{
        "type": "user_login",
        "user": "john_doe",
    })
    if err != nil {
        log.Printf("发布失败: %v", err)
    }
    
    // Pub/Sub 订阅消息
    go redisclient.Subscribe(context.Background(), "notifications", func(data string) error {
        log.Printf("收到通知: %s", data)
        return nil
    })
    
    // 分布式锁使用
    mutex, err := redisclient.Lock("user:123:update")
    if err != nil {
        log.Printf("获取锁失败: %v", err)
        return
    }
    defer redisclient.UnlockSafe(mutex)
    
    // 执行需要同步的操作
    log.Println("执行受保护的操作...")
    time.Sleep(2 * time.Second)
}
```

### Watermill 示例

```go
package main

import (
    "context"
    "log"
    
    "github.com/ThreeDotsLabs/watermill/message"
    wmkafka "github.com/joepeak/golib-mq/watermillmq/wmkafka"
    _ "github.com/joepeak/golib-conf"
)

func main() {
    // 创建发布者
    publisher, err := wmkafka.NewPublisher()
    if err != nil {
        log.Fatal(err)
    }
    
    // 发布消息
    msg := message.NewMessage("test", []byte("Hello Watermill!"))
    err = publisher.Publish("test-topic", msg)
    if err != nil {
        log.Printf("发布失败: %v", err)
    }
    
    // 创建订阅者
    subscriber, err := wmkafka.NewSubscriber()
    if err != nil {
        log.Fatal(err)
    }
    
    // 订阅消息
    messages, err := subscriber.Subscribe(context.Background(), "test-topic")
    if err != nil {
        log.Fatal(err)
    }
    
    for msg := range messages {
        log.Printf("收到消息: %s", string(msg.Payload))
        msg.Ack()
    }
}
```

## Redis MQ vs Redis Client 对比

### 架构差异

| 特性 | Redis MQ (Asynq) | Redis Client (Pub/Sub) |
|------|------------------|------------------------|
| **消息模式** | 任务队列 (Queue) | 发布订阅 (Pub/Sub) |
| **消息持久化** | ✅ 持久化到Redis Lists | ❌ 非持久化，实时传递 |
| **可靠性** | ✅ 高可靠性，支持重试 | ⚠️ 可能丢失消息 |
| **延迟任务** | ✅ 支持延迟和定时任务 | ❌ 不支持 |
| **优先级队列** | ✅ 支持多优先级队列 | ❌ 不支持 |
| **消费确认** | ✅ ACK机制，确保处理 | ❌ 无确认机制 |
| **重试机制** | ✅ 内置重试和死信队列 | ❌ 需自行实现 |
| **分布式锁** | ❌ 不包含 | ✅ 集成redsync |
| **性能** | 中等（持久化开销） | 高（直接内存传递） |
| **复杂度** | 较高 | 简单 |

### 使用场景建议

#### 选择 Redis MQ (Asynq) 当：
- ✅ **异步任务处理**：邮件发送、文件处理、数据计算
- ✅ **可靠性要求高**：订单处理、支付通知、用户注册
- ✅ **需要延迟任务**：定时提醒、过期处理
- ✅ **需要重试机制**：网络请求、第三方服务调用
- ✅ **复杂业务流程**：工作流、状态机

#### 选择 Redis Client (Pub/Sub) 当：
- ✅ **实时通知**：在线状态、系统通知、实时聊天
- ✅ **数据同步**：缓存更新、配置变更广播
- ✅ **分布式协调**：服务发现、负载均衡通知
- ✅ **需要分布式锁**：并发控制、资源互斥
- ✅ **轻量级应用**：微服务间简单通信

#### ⚠️ 重要提醒

**Redis Client (Pub/Sub) 的限制**：
- 消费者下线期间的消息会**永久丢失**
- 不适合关键业务数据
- 建议仅用于非关键的通知场景

**Redis MQ (Asynq) 的优势**：
- 基于Redis Lists，消息持久化
- 消费者异常下线后任务可被其他消费者接管
- 支持任务监控、重试、死信队列

## 配置

### Kafka 配置

```yaml
mq:
  kafka:
    brokers: ["localhost:9092"]
    producer:
      max_message_bytes: 1000000
      compression_type: "gzip"
    consumer:
      group_id: "test-group"
      auto_offset_reset: "earliest"
```

### Redis 配置

#### Redis MQ (Asynq) 配置

```yaml
mq:
  redismq:
    redis:
      addr: "localhost:6379"
      password: ""
      db: 0
      enabledCluster: false
      enabledTls: false
      cluster:
        addrs: ["redis-1:6379", "redis-2:6379", "redis-3:6379"]
    consumer:
      concurrency: 10
      queues:
        - name: "critical"
          priority: 6
        - name: "default"
          priority: 3
        - name: "low"
          priority: 1
```

#### Redis Client 配置

```yaml
redis:
  addr: "localhost:6379"
  username: ""
  password: ""
  db: 0
  enabledCluster: false
  enabledTls: false
  cluster:
    addrs: ["redis-1:6379", "redis-2:6379"]
  lock:
    expiration: 3        # 锁过期时间(秒)
    retryTimes: 5        # 重试次数
    retryInterval: 200    # 重试间隔(毫秒)
```

## API 文档

### Kafka 接口

```go
// 生产者
func NewProducer() (*Producer, error)
func (p *Producer) SendMessage(topic string, message string) error
func (p *Producer) SendMessageAsync(topic string, message string) error

// 消费者
func NewConsumer(groupID string) (*Consumer, error)
func (c *Consumer) Subscribe(topic string, handler func([]byte) error) error
func (c *Consumer) Close() error
```

### Redis MQ 接口 (Asynq)

```go
// 管理器
func GetManager() *RedisMQManager
func (m *RedisMQManager) RegisterHandler(taskType string, handler asynq.HandlerFunc) error
func (m *RedisMQManager) EnqueueTask(taskType string, data any, opts ...asynq.Option) (*asynq.TaskInfo, error)
func (m *RedisMQManager) EnqueueTaskWithDelay(taskType string, data any, duration time.Duration) (*asynq.TaskInfo, error)
func (m *RedisMQManager) EnqueueTaskAt(taskType string, data any, t time.Time) (*asynq.TaskInfo, error)
func (m *RedisMQManager) EnqueueCriticalTask(taskType string, data any) (*asynq.TaskInfo, error)
func (m *RedisMQManager) EnqueueLowTask(taskType string, data any) (*asynq.TaskInfo, error)
func (m *RedisMQManager) Start(concurrency int, queues map[string]int) error
func (m *RedisMQManager) Stop()
func (m *RedisMQManager) IsRunning() bool
func (m *RedisMQManager) GetStats() map[string]interface{}

// 全局便捷函数
func EnqueueTask(taskType string, payload interface{}, options ...asynq.Option) (*asynq.TaskInfo, error)
func EnqueueTaskWithDelay(taskType string, payload interface{}, delay time.Duration) (*asynq.TaskInfo, error)
func EnqueueTaskAt(taskType string, payload interface{}, processAt time.Time) (*asynq.TaskInfo, error)
func EnqueueCriticalTask(taskType string, payload interface{}) (*asynq.TaskInfo, error)
func EnqueueLowTask(taskType string, payload interface{}) (*asynq.TaskInfo, error)
func RegisterTaskHandler(taskType string, handler asynq.HandlerFunc) error
func StartRedisMQServer(concurrency int, queues map[string]int) error
func StopRedisMQServer()
func GetRedisMQStats() map[string]interface{}

// 任务创建
func NewTask(name string, data any) (*asynq.Task, error)

// Redis 连接
func NewRedisClient() (redis.UniversalClient, error)
```

### Redis Client 接口 (Pub/Sub + 锁)

```go
// Pub/Sub
func Publish(ctx context.Context, channel string, message interface{}) error
func Subscribe(ctx context.Context, channel string, handler func(data string) error) error

// 分布式锁
func Lock(lockKey string) (*redsync.Mutex, error)
func LockWithOptions(lockKey string, opts LockOptions) (*redsync.Mutex, error)
func UnlockSafe(mutex *redsync.Mutex) error

// Redis 客户端
var DefaultClient redis.UniversalClient
```

## 项目结构

```
golib-mq/
├── kafkamq/              # Kafka 实现
│   ├── consumer/
│   │   └── consumer.go
│   ├── producer/
│   │   └── producer.go
│   └── kafkamq.go
├── redismq/              # Redis MQ 实现 (基于Asynq)
│   └── redismq.go        # 统一的Redis MQ实现
├── redisclient/          # Redis Client 实现 (Pub/Sub + 分布式锁)
│   ├── redisclient.go   # Redis 连接和配置
│   ├── pubsub.go        # Pub/Sub 发布订阅
│   └── locker.go        # 分布式锁
├── watermillmq/           # Watermill 实现
│   ├── wmkafka/
│   │   ├── manager.go
│   │   └── wmkafka.go
│   └── wmnats/
│       ├── manager.go
│       ├── wmnats.go
│       └── example_usage.go
├── util/                 # 工具函数
│   └── util.go
├── go.mod
├── go.sum
└── README.md
```

## 依赖

### Kafka
- `github.com/IBM/sarama` - Kafka 客户端
- `github.com/sirupsen/logrus` - 日志库
- `github.com/spf13/viper` - 配置管理

### Redis MQ (Asynq)
- `github.com/redis/go-redis/v9` - Redis 客户端
- `github.com/hibiken/asynq` - 异步任务队列
- `github.com/sirupsen/logrus` - 日志库
- `github.com/spf13/viper` - 配置管理

### Redis Client (Pub/Sub + 锁)
- `github.com/redis/go-redis/v9` - Redis 客户端
- `github.com/go-redsync/redsync/v4` - 分布式锁
- `github.com/sirupsen/logrus` - 日志库
- `github.com/spf13/viper` - 配置管理

### Watermill
- `github.com/ThreeDotsLabs/watermill` - 事件流框架
- `github.com/ThreeDotsLabs/watermill-kafka/v3` - Watermill Kafka 适配器
- `github.com/ThreeDotsLabs/watermill-nats/v2` - Watermill NATS 适配器

## 性能特点

- **Kafka**: 支持高并发，分区机制，适合大数据量场景
- **Redis MQ**: 可靠任务队列，支持重试和延迟任务，适合异步处理
- **Redis Client**: 高性能实时通信，适合轻量级通知和分布式锁
- **Watermill**: 事件驱动架构，支持流处理，适合微服务

## 最佳实践

1. **连接池**: 合理设置连接池大小
2. **错误处理**: 实现重试和降级机制
3. **监控**: 集成日志和指标收集
4. **优雅关闭**: 正确处理程序退出时的资源清理

## 使用示例

本README中的代码示例涵盖了各种消息队列的基本用法：

- **Kafka 示例**：生产者和消费者的基本实现
- **Redis MQ 示例**：基于Asynq的异步任务处理
- **Redis Client 示例**：Pub/Sub发布订阅和分布式锁
- **Watermill 示例**：事件驱动架构实现

更多详细用法请参考各包的文档和代码注释。

## 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支: `git checkout -b feature/amazing-feature`
3. 提交更改: `git commit -m 'Add amazing feature'`
4. 推送分支: `git push origin feature/amazing-feature`
5. 提交 Pull Request

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 作者

[@joepeak](https://github.com/joepeak)

## 更新日志

### v0.4.0
- ✨ 新增 Redis Client (Pub/Sub + 分布式锁)
- 📝 完善 Redis MQ vs Redis Client 对比文档
- 🔧 优化配置结构和示例代码
- 🚀 迁移 redisclient 从 golib-toolkit 到 golib-mq

### v0.3.0
- ✨ 新增 Watermill 支持
- 🔧 优化 Kafka 连接池
- 📝 完善 Redis 错误处理

### v0.2.0
- 🎉 初始版本发布
- 📦 Kafka 和 Redis 基础功能
