package kafka

import (
	"bytes"
	"log"
	"runtime"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
)

// implements the WriteableStore interface
type mocStore struct {
	data map[string][][]byte
	err  error
	kv   map[string][]byte
}

func (s *mocStore) WriteToKey(key string, data []byte) error {

	if s.err != nil {
		return s.err
	}
	if s.data == nil {
		s.data = map[string][][]byte{}
	}
	if s.data[key] == nil {
		s.data[key] = [][]byte{}
	}

	s.data[key] = append(s.data[key], data)

	return s.err
}

func (s *mocStore) GetMeta(key []byte) []byte {
	if s.kv == nil {
		return []byte{}
	}
	return s.kv[string(key)]
}

func (s *mocStore) PutMeta(key, value []byte) {

	if s.kv == nil {
		s.kv = map[string][]byte{}
	}
	s.kv[string(key)] = value
}

func (s *mocStore) Purge() {

}

type ktest struct {
	ExpectOFfset int64
	t            *testing.T
	topic        string
	store        *mocStore
	consumer     *mocks.Consumer
	kafka        *Kafka
	partition    int32
	pc           *mocks.PartitionConsumer
}

func newKtest(t *testing.T, topic string) *ktest {
	k := &ktest{
		t:     t,
		topic: topic,
		store: &mocStore{},
	}

	conf := NewConfig()
	conf.Topic = topic
	k.consumer = mocks.NewConsumer(t, nil)
	return k
}

func (kt *ktest) expectOffset(offset int64) *ktest {
	kt.ExpectOFfset = offset
	return kt
}

func (kt *ktest) sendMessage(key, value []byte) *ktest {
	pc := kt.getpc()
	pc.YieldMessage(&sarama.ConsumerMessage{
		Key:       key,
		Value:     value,
		Topic:     kt.topic,
		Partition: kt.partition,
	})

	return kt
}

func (kt *ktest) getpc() *mocks.PartitionConsumer {

	if kt.pc != nil {
		return kt.pc
	}
	kt.pc = kt.consumer.ExpectConsumePartition(kt.topic, kt.partition, kt.ExpectOFfset)
	return kt.pc
}

func (kt *ktest) close() {
	kt.pc.Close()
	kt.consumer.Close()
}

func TestKafkaConsumer(t *testing.T) {

	log.Println("TestKafkaConsumer")
	topic := "test1"
	key := "thekey"
	value := []byte("the value doesn't really matter")
	msg1 := &sarama.ConsumerMessage{
		Key:       []byte(key),
		Value:     value,
		Topic:     topic,
		Partition: 0,
		Offset:    0,
	}
	store := &mocStore{}

	conf := NewConfig()
	conf.Topic = topic
	mocConsumer := mocks.NewConsumer(t, nil)
	defer mocConsumer.Close()

	pc := mocConsumer.ExpectConsumePartition(topic, 0, sarama.OffsetOldest)
	pc.ExpectMessagesDrainedOnClose()
	pc.YieldMessage(msg1)
	defer pc.Close()

	k, err := NewKafka(store, conf, mocConsumer)
	if err != nil {
		t.Fatal(err)
	}
	k.Run()
	runtime.Gosched() // make sure producer goroutines have a chance to fill the consumer channels
	k.Close()

	keyData := store.data[key]
	if keyData == nil {
		t.Fatal("expected key to be written to")
	}
	if keyData[0] == nil {
		t.Fatal("expected first value to be written to")
	}
	if !bytes.Equal(keyData[0], value) {
		t.Errorf("expected: %s\ngot: %s", value, keyData[0])
	}
}

func TestPersistOffset(t *testing.T) {

	log.Println("TestPersistOffset")

	store := &mocStore{}
	conf := NewConfig()
	conf.Topic = "test"

	kt := newKtest(t, conf.Topic).
		expectOffset(sarama.OffsetOldest).
		sendMessage([]byte("foo"), []byte("bar")).
		sendMessage([]byte("foo2"), []byte("bar2")).
		sendMessage([]byte("foo3"), []byte("bar3"))

	k, err := NewKafka(store, conf, kt.consumer)
	if err != nil {
		t.Fatal(err)
	}
	err = k.Run()
	if err != nil {
		t.Fatal(err)
	}
	runtime.Gosched() // make sure producer goroutines have a chance to fill the consumer channels
	k.Close()
	kt.close()

	hwMark := kt.pc.HighWaterMarkOffset() - 1
	if k.Offset() != hwMark {
		t.Errorf("expected %d: got: %d", hwMark, k.Offset())
	}

	// re-load the kafka consumer and verify that the offset is still the same
	k = nil
	k, err = NewKafka(store, conf, kt.consumer)
	if err != nil {
		t.Fatal(err)
	}

	if k.Offset() != hwMark {
		t.Errorf("failed to persist. Expected: %d, got: %d", hwMark, k.Offset())
	}

}
