package transactional

//go:generate mockgen -destination=mocks/consumer_kafka.go -package=mocks github.com/Shopify/sarama SyncProducer

import (
	"context"
	"database/sql"
	"time"

	"github.com/Shopify/sarama"
)

type kafkaConsumer struct {
	active    bool
	db        *sql.DB
	kafka     sarama.SyncProducer
	workerNum uint
}

func NewKafkaConsumer(db *sql.DB, kafka sarama.SyncProducer, workerNum uint) *kafkaConsumer {
	return &kafkaConsumer{
		active:    true,
		db:        db,
		kafka:     kafka,
		workerNum: workerNum,
	}
}

func (c *kafkaConsumer) Run() {
	for i := uint(1); i <= c.workerNum; i++ {
		go func() {
			for {
				if err := c.Process(); err != nil {
					time.Sleep(2 * time.Minute)
				}
			}
		}()
	}

	for c.active {
	}
}

func (c *kafkaConsumer) run() {
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

	query := "SELECT id, topic, payload FROM kafka_events LIMIT 1 FOR UPDATE SKIP LOCKED"
	row := tx.QueryRow(query)

	var id int
	var topic string
	var payload string
	if err := row.Scan(&id, &topic, &payload); err != nil {
		tx.Rollback()
		return err
	}

	query = "DELETE FROM kafka_events where id = ?"
	_, err = tx.ExecContext(context.Background(), query, id)
	if err != nil {
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

	return tx.Commit()
}
