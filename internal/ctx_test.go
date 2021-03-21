package internal_test

import (
	"context"
	"testing"

	"github.com/danielbintar/transactional/internal"
)

func TestGetRequestID(t *testing.T) {
	t.Run("without id", func(t *testing.T) {
		id := internal.GetRequestID(context.Background())
		expected := "no-request-id"
		if id != expected {
			t.Fatalf("%s != %s", id, expected)
		}
	})

	t.Run("with id", func(t *testing.T) {
		id := internal.GetRequestID(context.WithValue(context.Background(), "X-Request-ID", "123"))
		expected := "123"
		if id != expected {
			t.Fatalf("%s != %s", id, expected)
		}
	})
}
