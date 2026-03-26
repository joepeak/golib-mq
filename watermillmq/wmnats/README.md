# NATS Watermill 消费者名称配置

## 概述

本模块支持为 NATS JetStream 消费者设置自定义名称，这对于以下场景非常重要：

- **持久化消费者**: 消费者重启后能够从上次消费的位置继续
- **多服务部署**: 不同服务使用不同的消费者名称避免冲突
- **负载均衡**: 同一消费者组的多个实例共享消息负载
- **监控和调试**: 通过消费者名称更容易识别和监控

## 配置方式

### 1. 通过配置文件

```yaml
mq:
  watermill:
    logger:
      debug: true
      trace: false
  natsmq:
    consumer:
      name: "my-service-consumer"  # 消费者名称
      concurrency: 3               # 并发数
    url: "nats://localhost:4222"
    retryConnect: true
    timeout: 30s
    reconnectDelay: 5
```

### 2. 通过代码配置

```go
config := &WmNatsConfig{
    Watermill: WatermillConfig{
        Logger: LoggerConfig{Debug: true, Trace: false},
    },
    Nats: NatsConfig{
        Consumer: ConsumerConfig{
            Name:        "my-custom-consumer", // 消费者名称
            Concurrency: 3,
        },
        URL:            "nats://localhost:4222",
        RetryConnect:   true,
        Timeout:        30 * time.Second,
        ReconnectDelay: 5,
    },
}

manager := NewManager(config)
```

## 使用示例

### 基本使用

```go
// 1. 创建管理器
manager, err := NewManagerWithConfigKey("mq")
if err != nil {
    log.Fatal(err)
}
defer manager.Close()

// 2. 定义消息处理函数
handler := func(ctx context.Context, msgData string) error {
    log.Printf("处理消息: %s", msgData)
    return nil
}

// 3. 启动消费者
ctx := context.Background()
go manager.Consume(ctx, "user.events", 3, handler)

// 4. 发布消息
manager.Publish("user.events", map[string]interface{}{
    "event": "user_created",
    "id":    123,
})
```

### 多服务配置

```yaml
# 用户服务
user_service_mq:
  natsmq:
    consumer:
      name: "user-service-consumer"
      concurrency: 2

# 订单服务  
order_service_mq:
  natsmq:
    consumer:
      name: "order-service-consumer"
      concurrency: 5
```

```go
// 用户服务
userManager, _ := NewManagerWithConfigKey("user_service_mq")
go userManager.Consume(ctx, "user.events", 2, userHandler)

// 订单服务
orderManager, _ := NewManagerWithConfigKey("order_service_mq")
go orderManager.Consume(ctx, "order.events", 5, orderHandler)
```

## 消费者名称的作用

### 1. JetStream Durable Consumer

设置消费者名称后，NATS JetStream 会创建一个持久化消费者：

- **消息持久化**: 消费者离线时消息会被保存
- **断点续传**: 重启后从上次消费位置继续
- **消息确认**: 支持消息的 ACK/NACK 机制

### 2. 负载均衡

多个相同名称的消费者实例会形成消费者组：

```go
// 实例1
manager1.Consume(ctx, "events", 2, handler) // consumer: "my-service"

// 实例2  
manager2.Consume(ctx, "events", 2, handler) // consumer: "my-service"

// 消息会在两个实例间负载均衡
```

### 3. 消息分发策略

- **轮询分发**: 消息按轮询方式分发给消费者组内的实例
- **故障转移**: 某个实例失败时，消息会重新分发给其他实例
- **扩缩容**: 可以动态增减消费者实例数量

## 最佳实践

### 1. 命名规范

```go
// 推荐的命名格式
"service-name-consumer"     // 服务级别
"service-name-topic-consumer" // 服务+主题级别
"env-service-consumer"      // 环境+服务级别
```

### 2. 环境隔离

```yaml
# 开发环境
dev_mq:
  natsmq:
    consumer:
      name: "dev-user-service-consumer"

# 生产环境      
prod_mq:
  natsmq:
    consumer:
      name: "prod-user-service-consumer"
```

### 3. 监控配置

```go
// 启用详细日志用于监控
config := &WmNatsConfig{
    Watermill: WatermillConfig{
        Logger: LoggerConfig{
            Debug: true,  // 开发环境启用
            Trace: false, // 生产环境关闭
        },
    },
    // ...
}
```

### 4. 错误处理

```go
handler := func(ctx context.Context, msgData string) error {
    if err := processMessage(msgData); err != nil {
        log.Printf("处理消息失败: %v, 消息: %s", err, msgData)
        // 返回错误会触发 NACK，消息会重新投递
        return err
    }
    // 返回 nil 会触发 ACK，消息被确认消费
    return nil
}
```

## 故障排查

### 1. 消费者未收到消息

检查消费者名称是否正确配置：

```bash
# 查看 NATS 服务器上的消费者
nats consumer ls
nats consumer info <stream> <consumer>
```

### 2. 消息重复消费

确保消费者名称在不同服务间是唯一的：

```go
// 错误：两个不同服务使用相同消费者名称
userService.consumer.name = "service-consumer"
orderService.consumer.name = "service-consumer" // 冲突！

// 正确：使用不同的消费者名称
userService.consumer.name = "user-service-consumer"
orderService.consumer.name = "order-service-consumer"
```

### 3. 连接问题

检查 NATS 服务器配置和网络连接：

```go
config.Nats.RetryConnect = true
config.Nats.Timeout = 30 * time.Second
config.Nats.ReconnectDelay = 5
```