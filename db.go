package transactional

//go:generate mockgen -source=db.go -destination=mocks/db.go -package=mocks

import (
	"context"
	"database/sql"
)

type DB interface {
	Begin() (Tx, error)
}

type Tx interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Commit() error
	Rollback() error
}
