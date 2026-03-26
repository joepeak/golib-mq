package util

import (
	"encoding/json"

	"github.com/sirupsen/logrus"
)

func ObjToJson(obj interface{}) string {

	if obj == nil {
		return ""
	}

	b, err := json.Marshal(obj)
	if err != nil {
		logrus.Error("ObjToJson, error, ", err)
		return ""
	}

	return string(b)
}
