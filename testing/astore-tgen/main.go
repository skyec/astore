package main

import (
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/skyec/astore/testing"
)

var blobs [][]byte

func main() {

	var nBlobs int
	var kafkaBrokers string
	var kafkaTopic string

	flag.IntVar(&nBlobs, "b", 100, "The number of content blobs to generate")
	flag.StringVar(&kafkaBrokers, "brokers", "localhost:9092", "List of Kafka brokers")
	flag.StringVar(&kafkaTopic, "topic", "astore-stress", "Kafka topic")
	flag.Parse()

	st := time.Now()
	blobs = testing.GenerateBlobs(nBlobs)
	tend := time.Now()
	log.Printf("Generation took: %dms", tend.Sub(st)/time.Millisecond)

	log.Println("Producing...")
	kafka(strings.Split(kafkaBrokers, ","), kafkaTopic)

}

func kafka(brokers []string, topic string) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatal("Error creating producer:", err)
	}

	wg := sync.WaitGroup{}
	ok := 0
	errors := 0

	wg.Add(1)
	go func() {
		defer wg.Done()
		for _ = range producer.Successes() {
			ok++
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println("Producer error:", err)
			errors++
		}
	}()

	st := time.Now()
	for i := range blobs {
		time.Sleep(time.Nanosecond * 500)

		if i%1000 == 0 {
			log.Println("Sending:", i)
		}
		msg := &sarama.ProducerMessage{Topic: topic, Value: sarama.ByteEncoder(blobs[i])}
		producer.Input() <- msg
	}
	producer.AsyncClose()
	wg.Wait()
	tend := time.Now()

	dms := tend.Sub(st) / time.Millisecond
	if dms < 1 {
		dms = 1
	}

	ds := tend.Sub(st) / time.Second
	if ds < 1 {
		ds = 1
	}

	log.Printf("Producing:")
	log.Printf("Produced: %d", len(blobs))
	log.Printf("Duration: %dms", dms)
	log.Printf("Rate: %d m/ms", int64(len(blobs))/int64(dms))
	log.Printf("Rate: %d m/s", int64(len(blobs))/int64(ds))
	log.Printf("OK: %d, (%d%%)", ok, int(ok/len(blobs)*100))
	log.Printf("Errors: %d, (%d%%)", errors, int(errors/len(blobs)*100))

}
