package transactional

//go:generate mockgen -destination=mocks/consumer_kafka.go -package=mocks github.com/Shopify/sarama SyncProducer

import (
	"context"
	"time"

	"github.com/Shopify/sarama"
)

type kafkaConsumer struct {
	active bool
	db     DB
	kafka  sarama.SyncProducer
}

func NewKafkaConsumer(db DB, kafka sarama.SyncProducer) *kafkaConsumer {
	return &kafkaConsumer{
		active: true,
		db:     db,
		kafka:  kafka,
	}
}

func (c *kafkaConsumer) Run() {
	for c.active {
		if err := c.Process(); err != nil {
			time.Sleep(2 * time.Minute)
		}
	}
}

func (c *kafkaConsumer) Shutdown() {
	c.active = false
}

func (c *kafkaConsumer) Process() error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	query := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
	row := tx.QueryRow(query)

	var id int
	var topic string
	var payload string
	if err := row.Scan(&id, &topic, &payload); err != nil {
		tx.Rollback()
		return err
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(payload),
	}
	_, _, err = c.kafka.SendMessage(msg)
	if err != nil {
		tx.Rollback()
		return err
	}

	query = "DELETE FROM kafka_events where id = ?"
	_, err = tx.ExecContext(context.Background(), query, id)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
