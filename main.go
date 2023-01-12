package main

import (
	"pulsar-test/consumer"
	"pulsar-test/producer"
	"time"
)

func main() {
	go producer.Run()
	time.Sleep(100 * time.Millisecond)
	consumer.Run()
}
