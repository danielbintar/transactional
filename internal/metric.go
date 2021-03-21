package internal

//go:generate mockgen -source=metric.go -destination=mocks/metric.go -package=mocks

import (
	"time"
)

type Metric interface {
	Timing(name string, value time.Duration, tags []string, rate float64) error
}