package astore

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const statsCountChanBuffer = 100
const statsLogInterval = 5

type stats struct {
	cWrites *counter
	cErrors *counter
}

func newStats() *stats {
	return &stats{
		cWrites: newCounter(),
		cErrors: newCounter(),
	}
}

func (st *stats) run() {
	st.cErrors.run()
	st.cWrites.run()

	go func() {
		var i int
		tick := time.Tick(time.Second)
		for _ = range tick {
			st.cErrors.tick()
			st.cWrites.tick()
			i++
			if i%statsLogInterval == 0 {
				log.Println("Write Stats:", st.cWrites)
				log.Println("Error Stats:", st.cErrors)
				i = 1
			}
		}
	}()
}

func (st *stats) countWrite() {
	st.cWrites.count()
}

func (st *stats) countError() {
	st.cErrors.count()
}

type countType int

type counter struct {
	total, oneSec, lastSec, i10 int64
	tenSecCounters              [10]int64
	chCount                     chan countType
	startTime                   int64
	wg                          *sync.WaitGroup
	unixNowFn                   func() int64
}

const (
	COUNT_TICK countType = iota
	COUNT_COUNT
)

func newCounter() *counter {
	return &counter{
		chCount:   make(chan countType, statsCountChanBuffer),
		wg:        &sync.WaitGroup{},
		unixNowFn: func() int64 { return time.Now().Unix() },
	}
}

func (c *counter) run() {
	c.startTime = c.unixNowFn()
	c.wg.Add(1)
	go func() {
		for cType := range c.chCount {
			switch cType {
			case COUNT_COUNT:
				c.total++
				c.oneSec++
				c.tenSecCounters[c.i10] = c.oneSec
			case COUNT_TICK:
				c.i10 = (c.i10 + 1) % 10
				c.lastSec = c.oneSec
				c.oneSec = 0
			}
		}
		c.wg.Done()
	}()
}

func (c *counter) count() {
	c.chCount <- COUNT_COUNT
}

func (c *counter) tick() {
	c.chCount <- COUNT_TICK
}

func (c *counter) String() string {
	now := c.unixNowFn()
	elapsed := now - c.startTime
	if elapsed == 0 {
		elapsed = 1
	}

	return fmt.Sprintf("total: %d, 1s: %d, 10s: %.2f, all time avg: %.2f",
		c.total,
		c.lastSec,
		avg(c.tenSecCounters[:]),
		float64(c.total)/float64(elapsed))
}

func (c *counter) close() {
	close(c.chCount)
	c.wg.Wait()
}

func avg(list []int64) float32 {
	var sum int64
	for i := 0; i < len(list); i++ {
		sum += list[i]
	}
	return float32(sum) / float32(len(list))
}
