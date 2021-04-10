package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/danielbintar/transactional"

	_ "github.com/go-sql-driver/mysql"
)

const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

var seededRand *rand.Rand = rand.New(rand.NewSource(time.Now().UnixNano()))

func main() {
	sql := newMySQL()
	defer sql.Close()

	publisher := transactional.NewPublisher()

	for i := 1; i <= 1_000; i++ {
		tx, err := sql.Begin()
		if err != nil {
			panic(err)
		}

		for j := 1; j <= 1_000; j++ {
			payload := fmt.Sprintf(`{"name":"%s","age":%d}`, randomName(), seededRand.Intn(100))
			if err := publisher.PublishKafka(context.Background(), tx, "topic1", payload); err != nil {
				tx.Rollback()
				panic(err)
			}
		}

		tx.Commit()
	}
}

func randomName() string {
	length := seededRand.Intn(15) + 1
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[seededRand.Intn(len(charset))]
	}
	return string(b)
}

func newMySQL() *sql.DB {
	db, err := sql.Open("mysql", "root:rootpw@tcp(127.0.0.1:3306)/transactional?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true&interpolateParams=true")

	if err != nil {
		panic(err)
	}

	return db
}
