package redismq

import (
	"encoding/json"
	"fmt"

	"github.com/hibiken/asynq"
)

// NewTask 创建任务
func NewTask(name string, data any) (*asynq.Task, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("new task, json marshal error, %s", err.Error())
	}

	return asynq.NewTask(name, payload), nil
}
