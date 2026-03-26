package wmnats

import (
	"context"
	"log"
	"time"

	"github.com/spf13/viper"
)

// ExampleUsage 展示如何使用配置的消费者名称
func ExampleUsage() {
	// 1. 设置配置
	viper.SetConfigType("yaml")
	viper.SetConfigFile("config_example.yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("读取配置文件失败:", err)
	}

	// 2. 使用默认配置创建管理器
	manager, err := NewManagerWithConfigKey("mq")
	if err != nil {
		log.Fatal("创建管理器失败:", err)
	}
	defer manager.Close()

	// 3. 创建消息处理函数
	messageHandler := func(ctx context.Context, msgData string) error {
		log.Printf("处理消息: %s", msgData)
		// 这里添加你的业务逻辑
		return nil
	}

	// 4. 启动消费者
	ctx := context.Background()
	topic := "user.events"
	concurrency := 3

	go func() {
		if err := manager.Consume(ctx, topic, concurrency, messageHandler); err != nil {
			log.Printf("消费消息失败: %v", err)
		}
	}()

	// 5. 发布测试消息
	testMessage := map[string]interface{}{
		"event_type": "user_created",
		"user_id":    12345,
		"timestamp":  time.Now().Unix(),
	}

	if err := manager.Publish(topic, testMessage); err != nil {
		log.Printf("发布消息失败: %v", err)
	}

	// 6. 批量发布消息
	messages := []interface{}{
		map[string]interface{}{"event": "event1", "data": "data1"},
		map[string]interface{}{"event": "event2", "data": "data2"},
		map[string]interface{}{"event": "event3", "data": "data3"},
	}

	if err := manager.PublishMsgs(topic, messages); err != nil {
		log.Printf("批量发布消息失败: %v", err)
	}

	// 等待一段时间让消息被处理
	time.Sleep(5 * time.Second)
}

// ExampleMultipleServices 展示如何为不同服务使用不同的消费者名称
func ExampleMultipleServices() {
	// 用户服务管理器
	userManager, err := NewManagerWithConfigKey("user_service_mq")
	if err != nil {
		log.Fatal("创建用户服务管理器失败:", err)
	}
	defer userManager.Close()

	// 订单服务管理器
	orderManager, err := NewManagerWithConfigKey("order_service_mq")
	if err != nil {
		log.Fatal("创建订单服务管理器失败:", err)
	}
	defer orderManager.Close()

	ctx := context.Background()

	// 用户服务消费者
	go func() {
		userHandler := func(ctx context.Context, msgData string) error {
			log.Printf("[用户服务] 处理消息: %s", msgData)
			return nil
		}
		if err := userManager.Consume(ctx, "user.events", 2, userHandler); err != nil {
			log.Printf("用户服务消费失败: %v", err)
		}
	}()

	// 订单服务消费者
	go func() {
		orderHandler := func(ctx context.Context, msgData string) error {
			log.Printf("[订单服务] 处理消息: %s", msgData)
			return nil
		}
		if err := orderManager.Consume(ctx, "order.events", 5, orderHandler); err != nil {
			log.Printf("订单服务消费失败: %v", err)
		}
	}()

	// 发布消息到不同主题
	userManager.Publish("user.events", map[string]interface{}{
		"type": "user_registered",
		"id":   123,
	})

	orderManager.Publish("order.events", map[string]interface{}{
		"type":     "order_created",
		"order_id": 456,
	})

	time.Sleep(10 * time.Second)
}

// ExampleProgrammaticConfig 展示如何通过代码设置消费者名称
func ExampleProgrammaticConfig() {
	config := &WmNatsConfig{
		Watermill: WatermillConfig{
			Logger: LoggerConfig{
				Debug: true,
				Trace: false,
			},
		},
		Nats: NatsConfig{
			Consumer: ConsumerConfig{
				Name:        "my-custom-consumer", // 自定义消费者名称
				Concurrency: 3,
			},
			URL:            "nats://localhost:4222",
			RetryConnect:   true,
			Timeout:        30 * time.Second,
			ReconnectDelay: 5,
		},
	}

	manager := NewManager(config)
	defer manager.Close()

	ctx := context.Background()
	handler := func(ctx context.Context, msgData string) error {
		log.Printf("[自定义消费者] 处理消息: %s", msgData)
		return nil
	}

	go func() {
		if err := manager.Consume(ctx, "custom.topic", 3, handler); err != nil {
			log.Printf("自定义消费者失败: %v", err)
		}
	}()

	// 发布测试消息
	manager.Publish("custom.topic", map[string]interface{}{
		"message": "Hello from custom consumer",
		"time":    time.Now(),
	})

	time.Sleep(5 * time.Second)
}
