package transactional

//go:generate mockgen -destination=mocks/consumer_kafka.go -package=mocks github.com/Shopify/sarama SyncProducer

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type kafkaConsumer struct {
	active    bool
	db        *sql.DB
	kafka     sarama.SyncProducer
	workerNum uint
	batch     uint
}

func NewKafkaConsumer(db *sql.DB, kafka sarama.SyncProducer, workerNum uint, batch uint) *kafkaConsumer {
	return &kafkaConsumer{
		active:    true,
		batch:     batch,
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

	if c.batch < 2 {
		row := tx.QueryRow("SELECT id, topic, payload FROM kafka_events LIMIT 1 FOR UPDATE SKIP LOCKED")

		var id int
		var topic string
		var payload string
		if err := row.Scan(&id, &topic, &payload); err != nil {
			tx.Rollback()
			return err
		}

		_, err = tx.ExecContext(context.Background(), "DELETE FROM kafka_events where id = ?", id)
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
	} else {
		rows, err := tx.Query(fmt.Sprintf("SELECT id, topic, payload FROM kafka_events LIMIT %d FOR UPDATE SKIP LOCKED", c.batch))
		if err != nil {
			tx.Rollback()
			return err
		}

		idAry := make([]string, 0)
		messages := make([]*sarama.ProducerMessage, 0)

		for rows.Next() {
			var id int
			var topic string
			var payload string
			if err := rows.Scan(&id, &topic, &payload); err != nil {
				tx.Rollback()
				return err
			}

			idAry = append(idAry, strconv.Itoa(id))
			messages = append(messages, &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(payload),
			})
		}
		if err := rows.Err(); err != nil {
			tx.Rollback()
			return err
		}

		ids := strings.Join(idAry, "','")
		_, err = tx.ExecContext(context.Background(), fmt.Sprintf(`DELETE FROM kafka_events where id IN ('%s')`, ids))
		if err != nil {
			tx.Rollback()
			return err
		}

		if err = c.kafka.SendMessages(messages); err != nil {
			tx.Rollback()
			return err
		}
	}

	return tx.Commit()
}
