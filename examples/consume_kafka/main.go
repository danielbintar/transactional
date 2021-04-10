package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/danielbintar/transactional"

	_ "github.com/go-sql-driver/mysql"

	"github.com/Shopify/sarama"
)

func main() {
	sql := newMySQL()
	defer sql.Close()

	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, nil)
	if err != nil {
		panic(err)
	}

	consumer := transactional.NewKafkaConsumer(sql, producer, 40)
	go consumer.Run()
	defer consumer.Shutdown()

	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Consumer running......")

	<-sigint

	fmt.Println("Shutting down consumer")
}

func newMySQL() *sql.DB {
	db, err := sql.Open("mysql", "root:rootpw@tcp(127.0.0.1:3306)/transactional?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=true&interpolateParams=true")

	if err != nil {
		panic(err)
	}

	return db
}
