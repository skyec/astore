package astore

import "testing"

type counterFixture struct {
	c       []countType
	result  string
	seconds int64
}

func TestStatsCounter(t *testing.T) {

	fixtures := []counterFixture{
		counterFixture{
			[]countType{},
			"total: 0, 1s: 0, 10s: 0.00, all time avg: 0.00", 0},

		counterFixture{
			[]countType{
				COUNT_COUNT, COUNT_TICK,
			},
			"total: 1, 1s: 1, 10s: 0.10, all time avg: 1.00", 1},

		counterFixture{
			[]countType{
				COUNT_COUNT, COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
			},
			"total: 3, 1s: 3, 10s: 0.30, all time avg: 3.00", 1},

		counterFixture{
			[]countType{
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
			},
			"total: 4, 1s: 2, 10s: 0.40, all time avg: 2.00", 2},

		counterFixture{
			[]countType{
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_COUNT, // no closing tick
			},
			"total: 7, 1s: 2, 10s: 0.70, all time avg: 2.33", 3},

		counterFixture{
			[]countType{
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_COUNT, COUNT_TICK},
			"total: 6, 1s: 3, 10s: 0.60, all time avg: 2.00",
			3,
		},

		counterFixture{
			[]countType{
				// previous 10 seconds
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_TICK,
				// last 10 seconds
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_TICK,
				COUNT_COUNT, COUNT_COUNT, COUNT_TICK,
			},
			"total: 17, 1s: 2, 10s: 1.50, all time avg: 1.42",
			12,
		},
	}

	for _, fixture := range fixtures {
		c := newCounter()
		c.run()

		for _, cType := range fixture.c {
			switch cType {
			case COUNT_COUNT:
				c.count()
			case COUNT_TICK:
				c.tick()
			}

		}
		c.close()
		c.unixNowFn = func() int64 {
			return c.startTime + fixture.seconds
		}
		if fixture.result != c.String() {
			t.Errorf("\nExpected: %s\nGot:      %s", fixture.result, c)
		}
	}

}
