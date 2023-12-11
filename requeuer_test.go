package work

import (
	"context"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
)

func TestRequeue(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 10, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("foo", 14, nil)
	assert.NoError(t, err)
	_, err = enqueuer.EnqueueIn("bar", 19, nil)
	assert.NoError(t, err)

	resetNowEpochSecondsMock()

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{"wat", "foo", "bar"}, noopLogger)
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 2, listSize(pool, redisKeyJobs(ns, "wat")))
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, "foo")))
	assert.EqualValues(t, 0, listSize(pool, redisKeyJobs(ns, "bar")))
	assert.EqualValues(t, 2, zsetSize(pool, redisKeyScheduled(ns)))

	j := jobOnQueue(pool, redisKeyJobs(ns, "foo"))
	assert.Equal(t, j.Name, "foo")

	// Because we mocked time to 10 seconds ago above, the job was put on the zset with t=10 secs ago
	// We want to ensure it's requeued with t=now.
	// On boundary conditions with the VM, nowEpochSeconds() might be 1 or 2 secs ahead of EnqueuedAt
	assert.True(t, (j.EnqueuedAt+2) >= nowEpochSeconds())

}

func TestRequeueUnknown(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	tMock := nowEpochSeconds() - 10
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	enqueuer := NewEnqueuer(ns, pool)
	_, err := enqueuer.EnqueueIn("wat", -9, nil)
	assert.NoError(t, err)

	nowish := nowEpochSeconds()
	setNowEpochSecondsMock(nowish)

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{"bar"}, noopLogger)
	re.start()
	re.drain()
	re.stop()

	assert.EqualValues(t, 0, zsetSize(pool, redisKeyScheduled(ns)))
	assert.EqualValues(t, 1, zsetSize(pool, redisKeyDead(ns)))

	rank, job := jobOnZset(pool, redisKeyDead(ns))

	assert.Equal(t, nowish, rank)
	assert.Equal(t, nowish, job.FailedAt)
	assert.Equal(t, "unknown job when requeueing", job.LastErr)
}

func TestRequeuePeriodic(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	jobName := "clean"
	jobSpec := "*/1 * * * * *"
	shedule, err := cron.NewParser(cronFormat).Parse(jobSpec)
	assert.NoError(t, err)

	jobs := []*periodicJob{
		{jobName: jobName, spec: jobSpec, schedule: shedule},
	}

	enq := newPeriodicEnqueuer(ns, pool, jobs, noopLogger)
	enq.start()
	enq.stop()

	// 300 seconds have passed
	tMock := nowEpochSeconds() + 300
	setNowEpochSecondsMock(tMock)
	defer resetNowEpochSecondsMock()

	re := newRequeuer(ns, pool, redisKeyScheduled(ns), []string{jobName}, noopLogger)
	re.start()
	re.drain()
	re.stop()

	llen := listSize(pool, redisKeyJobs(ns, jobName))
	assert.Equal(t, int64(0), llen)
}

func TestRequeueSlowJob(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)

	jobName := "test_job"
	jobSpec := "*/1 * * * * *"

	wp := NewWorkerPool(struct{}{}, 1, ns, pool)
	defer wp.Stop()

	wp.PeriodicallyEnqueue(jobSpec, jobName)

	block := make(chan struct{})
	runned := make(chan struct{})

	wp.JobWithOptions(jobName, JobOptions{MaxConcurrency: 1}, func(context.Context, *Job) error {
		close(runned)
		<-block
		return nil
	})

	wp.Start()
	defer func() {
		close(block)
		wp.Stop()
	}()
	<-runned

	time.Sleep(time.Second * 2)

	llen := listSize(pool, redisKeyJobs(ns, jobName))
	assert.LessOrEqual(t, llen, int64(1))
}
