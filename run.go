package work

import (
	"context"
	"fmt"
	"reflect"
)

// runJob returns an error if the job fails, or there's a panic, or we couldn't
// reflect correctly. if we return an error, it signals we want the job to be retried.
func runJob(job *Job, ctxType reflect.Type, middleware []*middlewareHandler, jt *jobType) (returnCtx reflect.Value, returnError error) {
	returnCtx = reflect.New(ctxType)
	ctx := job.extractTraceContext(context.Background())

	currentMiddleware := 0
	maxMiddleware := len(middleware)

	var next NextMiddlewareFunc
	next = func() error {
		if currentMiddleware < maxMiddleware {
			mw := middleware[currentMiddleware]
			currentMiddleware++
			if mw.isGeneric {
				switch mwh := mw.genericMiddleware.(type) {
				case JobMiddleware:
					return mwh(job, next)
				case JobContextMiddleware:
					return mwh(ctx, job, next)
				}
			}

			res := mw.dynamicMiddleware.Call([]reflect.Value{returnCtx, reflect.ValueOf(job), reflect.ValueOf(next)})
			x := res[0].Interface()
			if x == nil {
				return nil
			}

			return x.(error)
		}

		if jt.isGeneric {
			switch h := jt.genericHandler.(type) {
			case JobHandler:
				return h(job)
			case JobContextHandler:
				return h(ctx, job)
			}
		}

		res := jt.dynamicHandler.Call([]reflect.Value{returnCtx, reflect.ValueOf(job)})

		x := res[0].Interface()
		if x == nil {
			return nil
		}

		return x.(error)
	}

	defer func() {
		if panicErr := recover(); panicErr != nil {
			// err turns out to be interface{}, of actual type "runtime.errorCString"
			// Luckily, the err sprints nicely via fmt.
			errorishError := fmt.Errorf("%v", panicErr)
			logError("runJob.panic", errorishError)
			returnError = errorishError
		}
	}()

	returnError = next()

	return
}
