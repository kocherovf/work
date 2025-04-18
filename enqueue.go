package work

import (
	"context"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Enqueuer can enqueue jobs.
type Enqueuer struct {
	Namespace string // eg, "myapp-work"
	Pool      Pool

	queuePrefix           string // eg, "myapp-work:jobs:"
	enqueueUniqueScript   *redis.Script
	enqueueUniqueInScript *redis.Script

	mtx       sync.RWMutex
	knownJobs map[string]int64
}

// NewEnqueuer creates a new enqueuer with the specified Redis namespace and Redis pool.
func NewEnqueuer(namespace string, pool Pool) *Enqueuer {
	if pool == nil {
		panic("NewEnqueuer needs a non-nil Pool")
	}

	return &Enqueuer{
		Namespace:             namespace,
		Pool:                  pool,
		queuePrefix:           redisKeyJobsPrefix(namespace),
		knownJobs:             make(map[string]int64),
		enqueueUniqueScript:   redis.NewScript(2, redisLuaEnqueueUnique),
		enqueueUniqueInScript: redis.NewScript(2, redisLuaEnqueueUniqueIn),
	}
}

// Enqueue will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) Enqueue(jobName string, args Q) (*Job, error) {
	return e.EnqueueContext(context.Background(), jobName, args)
}

// EnqueueContext will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", work.Q{"addr": "test@example.com"})
func (e *Enqueuer) EnqueueContext(ctx context.Context, jobName string, args Q) (*Job, error) {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	job.injectTraceContext(ctx)

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", e.queuePrefix+jobName, rawJSON); err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return job, err
	}

	return job, nil
}

// EnqueueBatchContext will enqueue the specified job name and arguments. The args param can be nil if no args ar needed.
// Example: e.Enqueue("send_email", []work.Q{{"addr": "test@example.com"}})
func (e *Enqueuer) EnqueueBatchContext(ctx context.Context, jobName string, argsList []Q) ([]*Job, error) {
	jobs := make([]*Job, 0, len(argsList))
	redisArgs := make([]any, 0, len(argsList)+1)
	redisArgs = append(redisArgs, e.queuePrefix+jobName)
	for _, args := range argsList {
		job := &Job{
			Name:       jobName,
			ID:         makeIdentifier(),
			EnqueuedAt: nowEpochSeconds(),
			Args:       args,
		}
		jobs = append(jobs, job)
		job.injectTraceContext(ctx)

		rawJSON, err := job.serialize()
		if err != nil {
			return nil, err
		}
		redisArgs = append(redisArgs, rawJSON)
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if _, err := conn.Do("LPUSH", redisArgs...); err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return jobs, err
	}

	return jobs, nil
}

// EnqueueIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueIn(jobName string, secondsFromNow int64, args map[string]interface{}) (*ScheduledJob, error) {
	return e.EnqueueContextIn(context.Background(), jobName, secondsFromNow, args)
}

// EnqueueContextIn enqueues a job in the scheduled job queue for execution in secondsFromNow seconds.
func (e *Enqueuer) EnqueueContextIn(ctx context.Context, jobName string, secondsFromNow int64, args Q) (*ScheduledJob,
	error) {
	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
	}

	job.injectTraceContext(ctx)

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	scheduledJob := &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	_, err = conn.Do("ZADD", redisKeyScheduled(e.Namespace), scheduledJob.RunAt, rawJSON)
	if err != nil {
		return nil, err
	}

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return scheduledJob, err
	}

	return scheduledJob, nil
}

// EnqueueUnique enqueues a job unless a job is already enqueued with the same name and arguments.
// The already-enqueued job can be in the normal work queue or in the scheduled job queue.
// Once a worker begins processing a job, another job with the same name and arguments can be enqueued again.
// Any failed jobs in the retry queue or dead queue don't count against the uniqueness -- so if a job fails and is retried, two unique jobs with the same name and arguments can be enqueued at once.
// In order to add robustness to the system, jobs are only unique for 24 hours after they're enqueued. This is mostly relevant for scheduled jobs.
// EnqueueUnique returns the job if it was enqueued and nil if it wasn't
func (e *Enqueuer) EnqueueUnique(jobName string, args Q) (*Job, error) {
	return e.EnqueueContextUnique(context.Background(), jobName, args)
}

// EnqueueContextUnique does the same as EnqueueUnique with context propagation.
func (e *Enqueuer) EnqueueContextUnique(ctx context.Context, jobName string, args Q) (*Job, error) {
	uniqueKey, err := redisKeyUniqueJob(e.Namespace, jobName, args)
	if err != nil {
		return nil, err
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
		Unique:     true,
	}

	job.injectTraceContext(ctx)

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return nil, err
	}

	scriptArgs := make([]interface{}, 0, 3)
	scriptArgs = append(scriptArgs, e.queuePrefix+jobName) // KEY[1]
	scriptArgs = append(scriptArgs, uniqueKey)             // KEY[2]
	scriptArgs = append(scriptArgs, rawJSON)               // ARGV[1]

	res, err := redis.String(e.enqueueUniqueScript.Do(conn, scriptArgs...))
	if res == "ok" && err == nil {
		return job, nil
	}

	return nil, err
}

// EnqueueUniqueIn enqueues a unique job in the scheduled job queue for execution in secondsFromNow seconds. See EnqueueUnique for the semantics of unique jobs.
func (e *Enqueuer) EnqueueUniqueIn(jobName string, secondsFromNow int64, args Q) (*ScheduledJob, error) {
	return e.EnqueueContextUniqueIn(context.Background(), jobName, secondsFromNow, args)
}

// // EnqueueContextUniqueIn does the same as EnqueueUniqueIn with context propagation.
func (e *Enqueuer) EnqueueContextUniqueIn(ctx context.Context, jobName string, secondsFromNow int64,
	args Q) (*ScheduledJob, error) {
	uniqueKey, err := redisKeyUniqueJob(e.Namespace, jobName, args)
	if err != nil {
		return nil, err
	}

	job := &Job{
		Name:       jobName,
		ID:         makeIdentifier(),
		EnqueuedAt: nowEpochSeconds(),
		Args:       args,
		Unique:     true,
	}

	job.injectTraceContext(ctx)

	rawJSON, err := job.serialize()
	if err != nil {
		return nil, err
	}

	conn := e.Pool.Get()
	defer conn.Close()

	if err := e.addToKnownJobs(conn, jobName); err != nil {
		return nil, err
	}

	scheduledJob := &ScheduledJob{
		RunAt: nowEpochSeconds() + secondsFromNow,
		Job:   job,
	}

	scriptArgs := make([]interface{}, 0, 4)
	scriptArgs = append(scriptArgs, redisKeyScheduled(e.Namespace)) // KEY[1]
	scriptArgs = append(scriptArgs, uniqueKey)                      // KEY[2]
	scriptArgs = append(scriptArgs, rawJSON)                        // ARGV[1]
	scriptArgs = append(scriptArgs, scheduledJob.RunAt)             // ARGV[2]

	res, err := redis.String(e.enqueueUniqueInScript.Do(conn, scriptArgs...))

	if res == "ok" && err == nil {
		return scheduledJob, nil
	}

	return nil, err
}

func (e *Enqueuer) addToKnownJobs(conn redis.Conn, jobName string) error {
	needSadd := true
	now := time.Now().Unix()

	e.mtx.RLock()
	t, ok := e.knownJobs[jobName]
	e.mtx.RUnlock()

	if ok {
		if now < t {
			needSadd = false
		}
	}
	if needSadd {
		if _, err := conn.Do("SADD", redisKeyKnownJobs(e.Namespace), jobName); err != nil {
			return err
		}

		e.mtx.Lock()
		e.knownJobs[jobName] = now + 300
		e.mtx.Unlock()
	}

	return nil
}
