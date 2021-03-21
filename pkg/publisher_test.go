package pkg_test

import (
	"context"
	"errors"
	"testing"

	mockInternal "github.com/danielbintar/transactional/internal/mocks"
	mockPkg "github.com/danielbintar/transactional/pkg/mocks"
	"github.com/danielbintar/transactional/pkg"

	"github.com/golang/mock/gomock"

	"go.uber.org/zap"
)

func TestPublisherWithSkipLockedPublish(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	log := mockInternal.NewMockLogZap(ctrl)
	metric := mockInternal.NewMockMetric(ctrl)
	db := mockPkg.NewMockDB(ctrl)

	publisher := pkg.NewPublisherWithSkipLocked(log, metric)
	ctx := context.Background()
	expectedQuery := "INSERT INTO transactional_events (topic, payload) VALUES(?,?)"

	t.Run("success", func(t *testing.T) {
		db.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload")
		metric.EXPECT().Timing("backend_latency", gomock.Any(), []string{"category:sql", "sub:publish", "status:ok"}, float64(1))
		log.EXPECT().Info("Success publish event",
			zap.Strings("tags", []string{"sql", "publish"}),
			zap.String("request_id", "no-request-id"),
			gomock.Any(),
		)

		publisher.Publish(ctx, db, "topic", "payload")
	})

	t.Run("failed", func(t *testing.T) {
		db.EXPECT().ExecContext(ctx, expectedQuery, "topic", "payload").Return(nil, errors.New("connection refused"))
		metric.EXPECT().Timing("backend_latency", gomock.Any(), []string{"category:sql", "sub:publish", "status:fail"}, float64(1))
		log.EXPECT().Info("Failed publish event",
			zap.String("error", "connection refused"),
			zap.Strings("tags", []string{"sql", "publish"}),
			zap.String("request_id", "no-request-id"),
			gomock.Any(),
		)

		publisher.Publish(ctx, db, "topic", "payload")
	})
}
