package pkg_test

import (
	"context"
	"errors"
	"testing"

	"github.com/danielbintar/transactional/pkg"
	mockPkg "github.com/danielbintar/transactional/pkg/mocks"

	"github.com/golang/mock/gomock"

	"github.com/stretchr/testify/assert"
)

func TestKafkaPublisherPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	db := mockPkg.NewMockDB(ctrl)

	publisher := pkg.NewKafkaPublisher()
	ctx := context.Background()
	expectedQuery := "INSERT INTO transactional_events (topic, payload) VALUES(?,?)"

	t.Run("success", func(t *testing.T) {
		db.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload")

		err := publisher.Publish(ctx, db, "topic", "payload")

		assert.Equal(t, nil, err, "publish success should not return error")
	})

	t.Run("failed", func(t *testing.T) {
		msg := errors.New("connection refused")
		db.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload").Return(nil, msg)

		err := publisher.Publish(ctx, db, "topic", "payload")

		assert.Equal(t, msg, err, "publish failed should return error")
	})
}
