package internal

import (
	"context"
)

func GetRequestID(ctx context.Context) string {
	if id, ok := ctx.Value("X-Request-ID").(string); ok {
		return id
	}

	return "no-request-id"
}
