package transactional_test

import (
	"context"
	"errors"
	"testing"

	"github.com/danielbintar/transactional"
	"github.com/danielbintar/transactional/mocks"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestPublisherPublishMWS(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx := mocks.NewMockTx(ctrl)

	publisher := transactional.NewPublisher()
	ctx := context.Background()
	expectedQuery := "INSERT INTO mws_events (topic, payload) VALUES(?,?)"

	t.Run("success", func(t *testing.T) {
		tx.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload")

		err := publisher.PublishMWS(ctx, tx, "topic", "payload")

		assert.Equal(t, nil, err, "publish success should not return error")
	})

	t.Run("failed", func(t *testing.T) {
		msg := errors.New("connection refused")
		tx.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload").Return(nil, msg)

		err := publisher.PublishMWS(ctx, tx, "topic", "payload")

		assert.Equal(t, msg, err, "publish failed should return error")
	})
}

func TestPublisherPublishKafka(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tx := mocks.NewMockTx(ctrl)

	publisher := transactional.NewPublisher()
	ctx := context.Background()
	expectedQuery := "INSERT INTO kafka_events (topic, payload) VALUES(?,?)"

	t.Run("success", func(t *testing.T) {
		tx.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload")

		err := publisher.PublishKafka(ctx, tx, "topic", "payload")

		assert.Equal(t, nil, err, "publish success should not return error")
	})

	t.Run("failed", func(t *testing.T) {
		msg := errors.New("connection refused")
		tx.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload").Return(nil, msg)

		err := publisher.PublishKafka(ctx, tx, "topic", "payload")

		assert.Equal(t, msg, err, "publish failed should return error")
	})
}
