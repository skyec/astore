package astore

import (
	"fmt"
	"log"
	"math"
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

type counter struct {
	total, oneSec  int64
	tenSecCounters [10]int64
	chCount        chan struct{}
	chTick         chan struct{}
	startTime      time.Time
}

func newCounter() *counter {
	return &counter{
		chCount: make(chan struct{}, statsCountChanBuffer),
		chTick:  make(chan struct{}, statsCountChanBuffer),
	}
}

func (c *counter) run() {
	c.startTime = time.Now()
	go func() {
		for {
			var i10 int64
			select {
			case <-c.chCount:
				c.total++
				c.oneSec++
				c.tenSecCounters[i10] = c.oneSec
			case <-c.chTick:
				i10 = (i10 + 1) % 10
				c.oneSec = 0
			}
		}
	}()
}

func (c *counter) count() {
	c.chCount <- struct{}{}
}

func (c *counter) tick() {
	c.chTick <- struct{}{}
}

func (c *counter) String() string {
	return fmt.Sprintf("total: %d, 1s: %d, 10s: %.2f, all time avg: %.2f",
		c.total,
		c.oneSec,
		avg(c.tenSecCounters[:]),
		c.total/int64(math.Ceil(float64(time.Now().Sub(c.startTime)/time.Second))))

}

func avg(list []int64) float32 {
	var sum int64
	for i := 0; i < len(list); i++ {
		sum += list[i]
	}
	return float32(sum / int64(len(list)))
}
