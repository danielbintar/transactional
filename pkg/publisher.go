package pkg

import (
	"context"
)

type Publisher interface {
	Publish(ctx context.Context, db DB, topic string, payload string) error
}

type kafkaPublisher struct {
}

func NewKafkaPublisher() kafkaPublisher {
	return kafkaPublisher{}
}

func (k kafkaPublisher) Publish(ctx context.Context, db DB, topic string, payload string) error {
	query := "INSERT INTO transactional_events (topic, payload) VALUES(?,?)"
	_, err := db.ExecContext(ctx, query, topic, payload)
	return err
}
