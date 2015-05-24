// kafka implements a consumer that pulls events from a kafka topic and adds them to the store
//
// The kafka partition key is used as the key for the astore.
//
// The append store tries to preserve the order of writes as much as possible. To aid this, only
// one consumer per store should pull from a single topic and a topic should only have a single
// partition. This consumer only uses partition `0`.
package kafka

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/skyec/astore"
)

const (
	defaultPartition = 0
	offsetKey        = "kafka.lastoffset"
)

type Config struct {
	Brokers []string // List of brokers in host:port format
	Topic   string   // Kafka topic to consume
}

func NewConfig() *Config {
	return &Config{}
}

type Kafka struct {
	master    sarama.Consumer
	pconsumer sarama.PartitionConsumer
	config    *Config
	chdone    chan struct{}
	store     astore.WriteableStore
	wg        *sync.WaitGroup
	offset    int64
}

func NewKafka(store astore.WriteableStore, config *Config, master sarama.Consumer) (*Kafka, error) {

	var err error
	if master == nil {
		master, err = sarama.NewConsumer(config.Brokers, nil)
		if err != nil {
			return nil, err
		}
	}

	k := &Kafka{
		config: config,
		master: master,
		chdone: make(chan struct{}, 1),
		store:  store,
		wg:     &sync.WaitGroup{},
	}
	k.offset = k.extractOffset()
	return k, nil
}

func (k *Kafka) Run() (err error) {

	k.pconsumer, err = k.master.ConsumePartition(k.config.Topic, defaultPartition, k.offset)
	if err != nil {
		return fmt.Errorf("Run(): %s", err)
	}
	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		for {
			select {
			case msg := <-k.pconsumer.Messages():

				logConsummedMessage(msg)
				err := k.store.WriteToKey(string(msg.Key), msg.Value)

				// TODO: what to do with failed message?
				//       - don't store the offset
				//       - restart consuming from kafka with a backoff
				if err != nil {
					log.Println("ERROR: writing to store:", err)
				}

				k.offset = msg.Offset
				k.storeOffset()

			case <-k.chdone:
				return
			}
		}
	}()
	return
}

func (k *Kafka) Close() {

	close(k.chdone)
	k.wg.Wait()
	if k.pconsumer != nil {
		k.pconsumer.Close()
	}
	k.master.Close()

}

func (k *Kafka) Offset() int64 {
	return k.offset
}

func logConsummedMessage(msg *sarama.ConsumerMessage) {
	log.Printf("CONSUMED '%s' %d %d '%s'",
		msg.Topic,
		msg.Partition,
		msg.Offset,
		msg.Key)
}

func (k *Kafka) storeOffset() {

	// ignore any of the special offset values
	if k.offset < 0 {
		return
	}

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(k.offset))
	k.store.PutMeta([]byte(offsetKey), buf)
}

func (k *Kafka) extractOffset() int64 {

	b := k.store.GetMeta([]byte(offsetKey))
	if b == nil || len(b) == 0 {
		return sarama.OffsetOldest
	}
	return int64(binary.LittleEndian.Uint64(b))
}
