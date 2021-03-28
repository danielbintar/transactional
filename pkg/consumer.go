package pkg

import (
	"context"
	"strconv"
	"time"
)

type Consumer interface {
	Run()
	Shutdown()
}

type mwsConsumer struct {
	active bool
	db     DB
	mws    Mws
}

func NewMWSConsumer(db DB, mws Mws) *mwsConsumer {
	return &mwsConsumer{
		active: true,
		db:     db,
		mws:    mws,
	}
}

func (c *mwsConsumer) Run() {
	for c.active == true {
		if err := c.Process(); err != nil {
			time.Sleep(2 * time.Minute)
		}
	}
}

func (c *mwsConsumer) Shutdown() {
	c.active = false
}

func (c *mwsConsumer) Process() error {
	tx, err := c.db.Begin()
	if err != nil {
		return err
	}

	query := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
	row := tx.QueryRow(query)

	var id int
	var topic string
	var payload string
	if err := row.Scan(&id, &topic, &payload); err != nil {
		tx.Rollback()
		return err
	}

	jobID := strconv.Itoa(id)
	_, err = c.mws.PutJobWithID(topic, jobID, jobID, "normal", payload, 0, 5*time.Second)
	if err != nil {
		tx.Rollback()
		return err
	}

	query = "DELETE FROM mws_events where id = ?"
	_, err = tx.ExecContext(context.Background(), query, id)
	if err != nil {
		tx.Rollback()
		return err
	}

	return tx.Commit()
}
