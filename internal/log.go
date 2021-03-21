package internal

//go:generate mockgen -source=log.go -destination=mocks/log.go -package=mocks

import (
	"go.uber.org/zap"
)

type LogZap interface {
	Error(msg string, fields ...zap.Field)
	Info(msg string, fields ...zap.Field)
}
