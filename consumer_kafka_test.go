package transactional_test

import (
	"errors"
	"testing"

	"github.com/danielbintar/transactional"
	"github.com/danielbintar/transactional/mocks"

	"github.com/golang/mock/gomock"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestKafkaConsumerFailBeginTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	defaultErr := errors.New("fail")
	mock.ExpectBegin().WillReturnError(defaultErr)

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail begin tx")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerFailToScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail scan")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerFailToDeleteData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail delete")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerFailToKafka(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail to kafka")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerFailToCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
	mock.ExpectCommit().WillReturnError(defaultErr)

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail to commit")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM kafka_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessage(gomock.Any()).Return(int32(1), int64(2), nil)
	mock.ExpectCommit()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 1)

	err = consumer.Process()
	assert.Equal(t, nil, err, "process should return not error when everything run normal")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchFailToQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail query")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchFailToScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := "lala"
	topic := "topic1"
	payload := `{"foo":"bar"}`

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.NotNil(t, err, "process should return error when fail scan")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchFailToDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`
	defaultErr := errors.New("fail")

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2'\\)").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail delete")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchFailToKafka(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`
	defaultErr := errors.New("fail")

	rows := mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload)
	rows.AddRow(3, topic, payload)

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(rows)
	mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2','3'\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessages(gomock.Any()).Return(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail kafka")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchFailToCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`
	defaultErr := errors.New("fail")

	rows := mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload)
	rows.AddRow(3, topic, payload)

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(rows)
	mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2','3'\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessages(gomock.Any())
	mock.ExpectCommit().WillReturnError(defaultErr)

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail commit")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestKafkaConsumerBatchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	kafka := mocks.NewMockSyncProducer(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	topic := "topic1"
	payload := `{"foo":"bar"}`

	rows := mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload)
	rows.AddRow(3, topic, payload)

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(rows)
	mock.ExpectExec("DELETE FROM kafka_events where id IN \\('2','3'\\)").WillReturnResult(sqlmock.NewResult(1, 1))
	kafka.EXPECT().SendMessages(gomock.Any())
	mock.ExpectCommit()

	consumer := transactional.NewKafkaConsumer(db, kafka, 1, 2)

	err = consumer.Process()
	assert.Nil(t, err, "process should not return error")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}
