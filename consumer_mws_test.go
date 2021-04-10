package transactional_test

import (
	// "database/sql"
	"errors"
	"testing"
	"time"

	"github.com/danielbintar/transactional"
	"github.com/danielbintar/transactional/mocks"

	"github.com/golang/mock/gomock"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestMWSConsumerFailBeginTx(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	defaultErr := errors.New("fail")
	mock.ExpectBegin().WillReturnError(defaultErr)

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail begin tx")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestMWSConsumerFailToScan(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail scan")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestMWSConsumerFailToDeleteData(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

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
	mock.ExpectExec("DELETE FROM mws_events where id = ?").WillReturnError(defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail delete")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestMWSConsumerFailToMWS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	sID := "2"
	topic := "topic1"
	payload := `{"foo":"bar"}`

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM mws_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, defaultErr)
	mock.ExpectRollback()

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail to mws")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestMWSConsumerFailToCommit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	sID := "2"
	topic := "topic1"
	payload := `{"foo":"bar"}`

	defaultErr := errors.New("fail")
	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM mws_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, nil)
	mock.ExpectCommit().WillReturnError(defaultErr)

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, defaultErr, err, "process should return error when fail to commit")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}

func TestMWSConsumerSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mws := mocks.NewMockMws(ctrl)

	db, mock, err := sqlmock.New()
	if err != nil {
		panic(err)
	}
	defer db.Close()

	id := 2
	sID := "2"
	topic := "topic1"
	payload := `{"foo":"bar"}`

	mock.ExpectBegin()
	mock.ExpectQuery("a").WillReturnRows(mock.NewRows([]string{"id", "topic", "payload"}).AddRow(id, topic, payload))
	mock.ExpectExec("DELETE FROM mws_events where id = ?").WillReturnResult(sqlmock.NewResult(1, 1))
	mws.EXPECT().PutJobWithID(topic, sID, sID, "normal", payload, int64(0), 5*time.Second).Return(nil, nil)
	mock.ExpectCommit()

	consumer := transactional.NewMWSConsumer(db, mws)

	err = consumer.Process()
	assert.Equal(t, nil, err, "process should return not error when everything run normal")

	consumer.Shutdown()
	consumer.Run()

	if err := mock.ExpectationsWereMet(); err != nil {
		panic(err)
	}
}
