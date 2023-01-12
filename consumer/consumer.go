package consumer

import (
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"strconv"
	"sync"
	"time"
)

const (
	pulsarURL = "pulsar://localhost:6650"
	topicName = "test-topic-xxx"

	totalCheck    = 100
	checkDuration = 1000 * time.Millisecond
	warmUp        = 5 * time.Second

	consumerNum = 21 // 1 mainConsumer + 20 soloConsumer
)

var (
	wg   sync.WaitGroup
	hits = 0
)

func Run() {
	client := newClient()

	tickers := make([]*ticker, consumerNum)
	consumers := make([]pulsar.Consumer, consumerNum)
	for i := 0; i < consumerNum; i++ {
		tickers[i] = newTicker()
		consumers[i] = newConsumer(client, topicName, fmt.Sprintf("sub-%d", i))
	}
	for i := 0; i < consumerNum; i++ {
		go consume(consumers[i], tickers[i])
	}
	wg.Add(1)
	go check(tickers)
	wg.Wait()
	fmt.Printf("total:%d, hits:%d, percentage:%.2f%%, consumerNum:%d, checkDuration:%dms\n",
		totalCheck*(consumerNum-1), hits, float64(hits)/(consumerNum-1)/float64(totalCheck)*100, consumerNum-1, checkDuration/time.Millisecond)
}

func check(tickers []*ticker) {
	defer wg.Done()
	tmp := totalCheck
	t := time.NewTicker(checkDuration)
	time.Sleep(warmUp)
	for tmp > 0 {
		select {
		case <-t.C:
			mainTs := tickers[0].getCurTick()
			for i := 1; i < consumerNum; i++ {
				if mainTs == tickers[i].getCurTick() {
					hits++
				}
			}
			tmp--
			fmt.Println("counter:", tmp)
		}
	}
}

func newClient() pulsar.Client {
	opts := pulsar.ClientOptions{URL: pulsarURL}
	client, err := pulsar.NewClient(opts)
	if err != nil {
		panic(fmt.Errorf("create client err %s", err))
	}
	return client
}

func newConsumer(client pulsar.Client, topic, subName string) pulsar.Consumer {
	receiveCh := make(chan pulsar.ConsumerMessage, 1024)
	opts := pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subName,
		Type:                        0,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionLatest,
		MessageChannel:              receiveCh,
	}
	consumer, err := client.Subscribe(opts)
	if err != nil {
		panic(fmt.Errorf("create consumer err %s", err))
	}
	return consumer
}

func consume(consumer pulsar.Consumer, t *ticker) {
	for {
		select {
		case msg := <-consumer.Chan():
			ts, err := strconv.Atoi(string(msg.Payload()))
			if err != nil {
				continue // not ts msg
			}
			if ts < 0 {
				panic(fmt.Errorf("unexpected ts:%d", ts))
			}
			t.setCurTick(ts)
		}
	}
}
