# Golib MQ

Go 语言消息队列库，支持 Kafka、Redis、NATS 等多种消息队列，提供统一的消息处理接口。

## 功能特性

- 🚀 **多队列支持**: 支持 Kafka、Redis、NATS、Watermill 等主流消息队列
- 🔄 **统一接口**: 提供统一的 Producer 和 Consumer 接口，便于切换
- ⚡ **高性能**: 基于官方客户端，性能优化
- 🛡️ **错误处理**: 完善的错误处理和重连机制
- 📊 **监控集成**: 内置日志和监控支持
- 🔧 **灵活配置**: 支持多种配置方式和连接参数

## 支持的消息队列

| 队列类型 | 包路径 | 特性 |
|----------|--------|------|
| **Kafka** | `github.com/joepeak/golib-mq/kafkamq` | 高吞吐量，分区机制 |
| **Redis** | `github.com/joepeak/golib-mq/redismq` | 轻量级，持久化 |
| **Watermill-Kafka** | `github.com/joepeak/golib-mq/watermillmq/wmkafka` | 事件驱动，流处理 |
| **Watermill-NATS** | `github.com/joepeak/golib-mq/watermillmq/wmnats` | 云原生，微服务 |

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

### Redis 示例

```go
package main

import (
    "context"
    "log"
    
    "github.com/joepeak/golib-mq/redismq"
    _ "github.com/joepeak/golib-conf"
)

func main() {
    // 创建生产者
    producer := redismq.NewProducer()
    
    // 发送消息
    err := producer.SendMessageAsync("test-queue", "Hello Redis!")
    if err != nil {
        log.Printf("发送失败: %v", err)
    }
    
    // 创建消费者
    consumer := redismq.NewConsumer()
    
    // 处理消息
    consumer.Process("test-queue", func(message []byte) error {
        log.Printf("处理消息: %s", string(message))
        return nil
    })
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

```yaml
mq:
  redis:
    addr: "localhost:6379"
    password: ""
    db: 0
    pool_size: 10
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

### Redis 接口

```go
// 生产者
func NewProducer() *Producer
func (p *Producer) SendMessage(queue string, message interface{}) error
func (p *Producer) SendMessageAsync(queue string, message interface{}) error

// 消费者
func NewConsumer() *Consumer
func (c *Consumer) Process(queue string, handler func([]byte) error) error
func (c *Consumer) Start() error
func (c *Consumer) Stop() error
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
├── redismq/              # Redis 实现
│   ├── consumer/
│   │   └── consumer.go
│   ├── producer/
│   │   └── producer.go
│   └── redismq.go
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

### Redis
- `github.com/redis/go-redis/v9` - Redis 客户端
- `github.com/hibiken/asynq` - 异步任务队列
- `github.com/sirupsen/logrus` - 日志库

### Watermill
- `github.com/ThreeDotsLabs/watermill` - 事件流框架
- `github.com/ThreeDotsLabs/watermill-kafka/v3` - Watermill Kafka 适配器
- `github.com/ThreeDotsLabs/watermill-nats/v2` - Watermill NATS 适配器

## 性能特点

- **Kafka**: 支持高并发，分区机制，适合大数据量场景
- **Redis**: 轻量快速，支持持久化，适合中小型应用
- **Watermill**: 事件驱动架构，支持流处理，适合微服务

## 最佳实践

1. **连接池**: 合理设置连接池大小
2. **错误处理**: 实现重试和降级机制
3. **监控**: 集成日志和指标收集
4. **优雅关闭**: 正确处理程序退出时的资源清理

## 示例项目

查看 `examples/` 目录：

- [Kafka 基础示例](examples/kafka-basic/)
- [Redis 异步队列](examples/redis-async/)
- [Watermill 事件流](examples/watermill-stream/)

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

### v0.3.0
- ✨ 新增 Watermill 支持
- 🔧 优化 Kafka 连接池
- 📝 完善 Redis 错误处理

### v0.2.0
- 🎉 初始版本发布
- 📦 Kafka 和 Redis 基础功能
