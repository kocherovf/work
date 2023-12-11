package work

import (
	"context"
	"fmt"
	"reflect"
)

// runJob returns an error if the job fails, or there's a panic, or we couldn't
// reflect correctly. if we return an error, it signals we want the job to be retried.
func runJob(
	job *Job,
	ctxType reflect.Type,
	middlewares []*middlewareHandler,
	jt *jobType,
	logger StructuredLogger,
) (returnCtx reflect.Value, returnError error) {
	returnCtx = reflect.New(ctxType)
	ctx := job.extractTraceContext(context.Background())

	next := func() error {
		mw := chainMiddleware(returnCtx, middlewares)

		if jt.isGeneric {
			switch h := jt.genericHandler.(type) {
			case JobHandler:
				return mw(ctx, job, func(_ context.Context, j *Job) error { return h(j) })
			case JobContextHandler:
				return mw(ctx, job, h)
			}
		}

		return mw(ctx, job, func(_ context.Context, j *Job) error {
			res := jt.dynamicHandler.Call([]reflect.Value{returnCtx, reflect.ValueOf(job)})

			x := res[0].Interface()
			if x == nil {
				return nil
			}

			return x.(error)
		})
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			logger.Error("runJob.panic", errAttr(errorishError))
			returnError = errorishError
		}
	}()

	returnError = next()

	return
}

// chainMiddleware creates a single middleware out of a chain of many middlewares.
//
// Execution is done in left-to-right order, including passing of context.
// For example chainMiddleware(one, two, three) will execute one before two before
// three, and three will see context changes of one and two.
func chainMiddleware(returnCtx reflect.Value, middlewares []*middlewareHandler) JobContextMiddleware {
	n := len(middlewares)

	return func(ctx context.Context, j *Job, jch JobContextHandler) error {
		chainer := func(currentMw *middlewareHandler, currentHandler JobContextHandler) JobContextHandler {
			return func(currentCtx context.Context, j *Job) error {
				bareHandler := func() error {
					return currentHandler(currentCtx, j)
				}

				if currentMw.isGeneric {
					switch mwh := currentMw.genericMiddleware.(type) {
					case JobMiddleware:
						return mwh(j, bareHandler)
					case JobContextMiddleware:
						return mwh(currentCtx, j, currentHandler)
					}
				}

				res := currentMw.dynamicMiddleware.Call([]reflect.Value{returnCtx, reflect.ValueOf(j), reflect.ValueOf(bareHandler)})
				x := res[0].Interface()
				if x == nil {
					return nil
				}

				return x.(error)
			}
		}

		chainedHandler := jch
		for i := n - 1; i >= 0; i-- {
			chainedHandler = chainer(middlewares[i], chainedHandler)
		}

		return chainedHandler(ctx, j)
	}
}
