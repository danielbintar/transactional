package pkg

//go:generate mockgen -source=db.go -destination=mocks/db.go -package=mocks

import (
	"context"
	"database/sql"
)

type DB interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	CommitContext(ctx context.Context) error
	RollbackContext(ctx context.Context) error
}
