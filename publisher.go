package transactional

import (
	"context"
)

type Publisher interface {
	PublishMWS(ctx context.Context, tx Tx, topic string, payload string) error
	PublishKafka(ctx context.Context, tx Tx, topic string, payload string) error
}

type publisher struct {
}

func NewPublisher() publisher {
	return publisher{}
}

func (p publisher) PublishMWS(ctx context.Context, tx Tx, topic string, payload string) error {
	query := "INSERT INTO mws_events (topic, payload) VALUES(?,?)"
	_, err := tx.ExecContext(ctx, query, topic, payload)
	return err
}

func (p publisher) PublishKafka(ctx context.Context, tx Tx, topic string, payload string) error {
	query := "INSERT INTO kafka_events (topic, payload) VALUES(?,?)"
	_, err := tx.ExecContext(ctx, query, topic, payload)
	return err
}
