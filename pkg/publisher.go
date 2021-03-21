package pkg

import (
	"context"
	"time"

	"github.com/danielbintar/transactional/internal"

	"go.uber.org/zap"
)


type Publisher interface {
	Publish(ctx context.Context, db DB, topic string, payload string) error
}

type publisherWithSkipLocked struct {
	l internal.LogZap
	m internal.Metric
}

func NewPublisherWithSkipLocked(l internal.LogZap, m internal.Metric) publisherWithSkipLocked {
	return publisherWithSkipLocked{
		l: l,
		m: m,
	}
}

func (p publisherWithSkipLocked) Publish(ctx context.Context, db DB, topic string, payload string) error {
	start := time.Now()

	query := "INSERT INTO transactional_events (topic, payload) VALUES(?,?)"
	_, err := db.ExecContext(ctx, query, topic, payload)

	elapsed := time.Since(start)
	tags := []string{"category:sql", "sub:publish"}

	if err == nil {
		tags = append(tags, "status:ok")
		p.m.Timing("backend_latency", elapsed, tags, 1)
		p.l.Info("Success publish event",
			zap.Strings("tags", []string{"sql", "publish"}),
			zap.String("request_id", internal.GetRequestID(ctx)),
			zap.Int64("duration_in_ms", elapsed.Milliseconds()),
		)

		return nil
	}

	tags = append(tags, "status:fail")
	p.m.Timing("backend_latency", elapsed, tags, 1)
	p.l.Info("Failed publish event",
		zap.String("error", err.Error()),
		zap.Strings("tags", []string{"sql", "publish"}),
		zap.String("request_id", internal.GetRequestID(ctx)),
		zap.Int64("duration_in_ms", elapsed.Milliseconds()),
	)

	return err
}
