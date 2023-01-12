package producer

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

const (
	pulsarURL = "pulsar://localhost:6650"
	topicName = "test-topic-xxx"

	timeTick          = 200 * time.Millisecond
	noiseDataSize     = 5 * 1024 // kb
	noiseDataDuration = 100      // ms
)

func Run() {
	client := newClient()
	producer1 := newProducer(client)
	producer2 := newProducer(client)
	go produce(producer1)
	produceNoise(producer2)
}

func newClient() pulsar.Client {
	opts := pulsar.ClientOptions{URL: pulsarURL}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		panic(fmt.Errorf("create client err %s", err))
	}
	return client
}

func newProducer(client pulsar.Client) pulsar.Producer {
	opts := pulsar.ProducerOptions{Topic: topicName}
	producer, err := client.CreateProducer(opts)
	if err != nil {
		panic(fmt.Errorf("create producer err %s", err))
	}
	return producer
}

func produce(producer pulsar.Producer) {
	ts := 0
	ticker := time.NewTicker(timeTick)
	for {
		select {
		case <-ticker.C:
			_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload:    []byte(strconv.Itoa(ts)),
				Properties: map[string]string{},
			})
			if err != nil {
				panic(fmt.Errorf("producer send error %s", err))
			}
			ts++
		}
	}
}

func produceNoise(producer pulsar.Producer) {
	bytes := make([]byte, noiseDataSize*1024)
	rand.Read(bytes)
	for {
		select {
		case <-time.After(time.Duration(rand.Int63n(noiseDataDuration)) * time.Millisecond):
			_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
				Payload:    bytes,
				Properties: map[string]string{},
			})
			if err != nil {
				panic(fmt.Errorf("producer send error %s", err))
			}
		}
	}
}
