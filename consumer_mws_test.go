package transactional_test

import (
	"errors"
	"testing"
	"time"

	"github.com/danielbintar/transactional"
	"github.com/danielbintar/transactional/mocks"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestMWSConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db := mocks.NewMockDB(ctrl)
	mws := mocks.NewMockMws(ctrl)
	tx := mocks.NewMockTx(ctrl)
	row := mocks.NewMockSQLRow(ctrl)

	consumer := transactional.NewMWSConsumer(db, mws)

	defaultErr := errors.New("fail")
	id := 2
	sID := "2"
	topic := "topic1"
	payload := `{"foo":"bar"}`

	t.Run("fail begin trx", func(t *testing.T) {
		db.EXPECT().Begin().Return(tx, defaultErr)
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail begin tx")
	})

	t.Run("fail to scan", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("fail to mws", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail mws")
	})

	t.Run("fail to delete data", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM mws_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("fail to commit", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM mws_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, nil)
		tx.EXPECT().Commit().Return(defaultErr)
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("success", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM mws_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM mws_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, nil)
		tx.EXPECT().Commit().Return(nil)
		err := consumer.Process()
		assert.Equal(t, nil, err, "process should return error when fail scan")
	})

	consumer.Shutdown()
	consumer.Run()
}
