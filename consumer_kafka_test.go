package transactional_test

import (
	"errors"
	"testing"

	"github.com/danielbintar/transactional"
	"github.com/danielbintar/transactional/mocks"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestKafkaConsumer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db := mocks.NewMockDB(ctrl)
	kafka := mocks.NewMockSyncProducer(ctrl)
	tx := mocks.NewMockTx(ctrl)
	row := mocks.NewMockSQLRow(ctrl)

	consumer := transactional.NewKafkaConsumer(db, kafka)

	defaultErr := errors.New("fail")
	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	t.Run("fail begin trx", func(t *testing.T) {
		db.EXPECT().Begin().Return(tx, defaultErr)
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail begin tx")
	})

	t.Run("fail to scan", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).Return(defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("fail to kafka", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail mws")
	})

	t.Run("fail to delete data", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM kafka_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, defaultErr)
		tx.EXPECT().Rollback()
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("fail to commit", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM kafka_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, nil)
		tx.EXPECT().Commit().Return(defaultErr)
		err := consumer.Process()
		assert.Equal(t, defaultErr, err, "process should return error when fail scan")
	})

	t.Run("success", func(t *testing.T) {
		fetchQuery := "SELECT id, topic, payload FROM kafka_events FOR UPDATE SKIP LOCKED LIMIT 1"
		deleteQuery := "DELETE FROM kafka_events where id = ?"
		db.EXPECT().Begin().Return(tx, nil)
		tx.EXPECT().QueryRow(fetchQuery).Return(row)
		row.EXPECT().Scan(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(pID *int, pTopic *string, pPayload *string) error {
			*pID = id
			*pTopic = topic
			*pPayload = payload
			return nil
		})
		kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
		tx.EXPECT().ExecContext(gomock.Any(), deleteQuery, id).Return(nil, nil)
		tx.EXPECT().Commit().Return(nil)
		err := consumer.Process()
		assert.Equal(t, nil, err, "process should return error when fail scan")
	})

	consumer.Shutdown()
	consumer.Run()
}
