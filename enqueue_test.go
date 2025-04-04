package work

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestEnqueue(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	job, err := enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.Equal(t, "wat", job.Name)
	assert.True(t, len(job.ID) > 10)                        // Something is in it
	assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", job.ArgString("b"))
	assert.EqualValues(t, 1, job.ArgInt64("a"))
	assert.NoError(t, job.ArgError())

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, knownJobs(pool, redisKeyKnownJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the queue is 1
	assert.EqualValues(t, 1, listSize(pool, redisKeyJobs(ns, "wat")))

	// Get the job
	j := jobOnQueue(pool, redisKeyJobs(ns, "wat"))
	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())

	// Now enqueue another job, make sure that we can enqueue multiple
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	_, err = enqueuer.Enqueue("wat", Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	assert.EqualValues(t, 2, listSize(pool, redisKeyJobs(ns, "wat")))
}

func TestEnqueueContext(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	jobName := "myjob"
	cleanKeyspace(ns, pool)

	exp := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exp))

	ctx, span := tp.Tracer("").Start(context.Background(), "test trace")
	span.End()

	enqueuer := NewEnqueuer(ns, pool)

	j, err := enqueuer.EnqueueContext(ctx, jobName, Q{"asdf": "bcd"})
	require.NoError(t, err)
	assert.NotNil(t, j.TraceContext)

	job := jobOnQueue(pool, redisKeyJobs(ns, jobName))
	assert.Equal(t, j.TraceContext, job.TraceContext)
}

func TestEnqueueBatchContext(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	jobName := "myjob"
	cleanKeyspace(ns, pool)

	exp := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exp))

	ctx, span := tp.Tracer("").Start(context.Background(), "test trace")
	span.End()

	enqueuer := NewEnqueuer(ns, pool)

	js, err := enqueuer.EnqueueBatchContext(ctx, jobName, []Q{{"asdf": "1"}, {"asdf": "2"}})
	require.NoError(t, err)

	require.Len(t, js, 2)
	assert.NotNil(t, js[0].TraceContext)
	assert.NotNil(t, js[1].TraceContext)

	job := jobOnQueue(pool, redisKeyJobs(ns, jobName))
	assert.Equal(t, js[0].TraceContext, job.TraceContext)
	job = jobOnQueue(pool, redisKeyJobs(ns, jobName))
	assert.Equal(t, js[1].TraceContext, job.TraceContext)
}

func TestEnqueueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Set to expired value to make sure we update the set of known jobs
	enqueuer.knownJobs["wat"] = 4

	job, err := enqueuer.EnqueueIn("wat", 300, Q{"a": 1, "b": "cool"})
	assert.Nil(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	// Make sure "wat" is in the known jobs
	assert.EqualValues(t, []string{"wat"}, knownJobs(pool, redisKeyKnownJobs(ns)))

	// Make sure the cache is set
	expiresAt := enqueuer.knownJobs["wat"]
	assert.True(t, expiresAt > (time.Now().Unix()+290))

	// Make sure the length of the scheduled job queue is 1
	assert.EqualValues(t, 1, zsetSize(pool, redisKeyScheduled(ns)))

	// Get the job
	score, j := jobOnZset(pool, redisKeyScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290)
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
}

func TestEnqueueUnique(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)
	var mutex = &sync.Mutex{}
	job, err := enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
	}

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Process the queues. Ensure the right number of jobs were processed
	var wats, taws int64
	wp := NewWorkerPool(TestContext{}, 3, ns, pool)
	wp.JobWithOptions("wat", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		wats++
		mutex.Unlock()
		return nil
	})
	wp.JobWithOptions("taw", JobOptions{Priority: 1, MaxFails: 1}, func(job *Job) error {
		mutex.Lock()
		taws++
		mutex.Unlock()
		return fmt.Errorf("ohno")
	})
	wp.Start()
	wp.Drain()
	wp.Stop()

	assert.EqualValues(t, 3, wats)
	assert.EqualValues(t, 1, taws)

	// Enqueue again. Ensure we can.
	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUnique("wat", Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	// Even though taw resulted in an error, we should still be able to re-queue it.
	// This could result in multiple taws enqueued at the same time in a production system.
	job, err = enqueuer.EnqueueUnique("taw", nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}

func TestEnqueueUniqueIn(t *testing.T) {
	pool := newTestPool(":6379")
	ns := "work"
	cleanKeyspace(ns, pool)
	enqueuer := NewEnqueuer(ns, pool)

	// Enqueue two unique jobs -- ensure one job sticks.
	job, err := enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	if assert.NotNil(t, job) {
		assert.Equal(t, "wat", job.Name)
		assert.True(t, len(job.ID) > 10)                        // Something is in it
		assert.True(t, job.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
		assert.True(t, job.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
		assert.Equal(t, "cool", job.ArgString("b"))
		assert.EqualValues(t, 1, job.ArgInt64("a"))
		assert.NoError(t, job.ArgError())
		assert.EqualValues(t, job.EnqueuedAt+300, job.RunAt)
	}

	job, err = enqueuer.EnqueueUniqueIn("wat", 10, Q{"a": 1, "b": "cool"})
	assert.NoError(t, err)
	assert.Nil(t, job)

	// Get the job
	score, j := jobOnZset(pool, redisKeyScheduled(ns))

	assert.True(t, score > time.Now().Unix()+290) // We don't want to overwrite the time
	assert.True(t, score <= time.Now().Unix()+300)

	assert.Equal(t, "wat", j.Name)
	assert.True(t, len(j.ID) > 10)                        // Something is in it
	assert.True(t, j.EnqueuedAt > (time.Now().Unix()-10)) // Within 10 seconds
	assert.True(t, j.EnqueuedAt < (time.Now().Unix()+10)) // Within 10 seconds
	assert.Equal(t, "cool", j.ArgString("b"))
	assert.EqualValues(t, 1, j.ArgInt64("a"))
	assert.NoError(t, j.ArgError())
	assert.True(t, j.Unique)

	// Now try to enqueue more stuff and ensure it
	job, err = enqueuer.EnqueueUniqueIn("wat", 300, Q{"a": 1, "b": "coolio"})
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("wat", 300, nil)
	assert.NoError(t, err)
	assert.Nil(t, job)

	job, err = enqueuer.EnqueueUniqueIn("taw", 300, nil)
	assert.NoError(t, err)
	assert.NotNil(t, job)
}
