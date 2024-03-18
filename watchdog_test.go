package work

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCheckTimesHeap(t *testing.T) {
	t1, _ := time.Parse(time.RFC3339, "2019-01-01T00:00:00Z")
	t2, _ := time.Parse(time.RFC3339, "2020-01-01T00:00:00Z")
	t3, _ := time.Parse(time.RFC3339, "2021-01-01T00:00:00Z")

	tests := []struct {
		data     []time.Time
		expected []time.Time
	}{
		{
			[]time.Time{},
			[]time.Time{},
		},
		{
			[]time.Time{t1},
			[]time.Time{t1},
		},
		{
			[]time.Time{t1, t1},
			[]time.Time{t1},
		},
		{
			[]time.Time{t1, t2, t3},
			[]time.Time{t1, t2, t3},
		},
		{
			[]time.Time{t3, t3, t3},
			[]time.Time{t3},
		},
		{
			[]time.Time{t2, t1},
			[]time.Time{t1, t2},
		},
		{
			[]time.Time{t3, t2, t1},
			[]time.Time{t1, t2, t3},
		},
		{
			[]time.Time{t3, t3, t3, t2, t2, t1},
			[]time.Time{t1, t2, t3},
		},
	}

	for i, tt := range tests {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			h := newCheckTimesHeap()
			for _, v := range tt.data {
				h.Push(v)
			}
			require.Equal(t, len(tt.expected), h.Len())

			for i := 0; h.Len() > 0; i++ {
				tm := h.Pop()
				require.Equal(t, tt.expected[i], tm)
			}
		})
	}
}

func TestWatchdog(t *testing.T) {
	t.Parallel()

	require := require.New(t)

	const jobName = "test"
	j, err := newPeriodicJob("* * * * * *", jobName)
	require.NoError(err)

	w := newWatchdog(
		watchdogWithFailCheckingTimeout(time.Millisecond * 2000),
	)
	defer w.stop()

	w.addPeriodicJobs(j)
	w.start()
	w.planning(time.Now())

	time.Sleep(time.Millisecond * 2000)
	w.processedJobs <- &Job{
		Name:       jobName,
		EnqueuedAt: time.Now().Unix(),
	}
	time.Sleep(time.Millisecond * 500)

	require.Equal(WatchdogStat{Name: "test", Processed: 1, Skipped: 0}, w.stats()[0])

	time.Sleep(time.Millisecond * 1600)
	require.Equal(WatchdogStat{Name: "test", Processed: 1, Skipped: 1}, w.stats()[0])
}
