package pkg

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
	QueryRow(query string, args ...interface{}) SQLRow
	Commit() error
	Rollback() error
}

type SQLRow interface {
	Scan(dest ...interface{}) error
}
