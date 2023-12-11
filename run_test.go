package work

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunBasicMiddleware(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		j.setArg("mw1", "mw1")
		return next()
	}

	mw2 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record(j.Args["mw1"].(string))
		c.record("mw2")
		return next()
	}

	mw3 := func(c *tstCtx, j *Job, next NextMiddlewareFunc) error {
		c.record("mw3")
		return next()
	}

	mw4 := func(ctx context.Context, j *Job, next JobContextHandler) error {
		j.setArg("mw4", "mw4")
		return next(ctx, j)
	}

	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		c.record(j.Args["a"].(string))
		return nil
	}

	middleware := []*middlewareHandler{
		{isGeneric: true, genericMiddleware: mw1},
		{isGeneric: false, dynamicMiddleware: reflect.ValueOf(mw2)},
		{isGeneric: false, dynamicMiddleware: reflect.ValueOf(mw3)},
		{isGeneric: true, genericMiddleware: mw4},
	}

	jt := &jobType{
		Name:           "foo",
		isGeneric:      false,
		dynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
		Args: map[string]interface{}{"a": "foo"},
	}

	v, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
	assert.NoError(t, err)
	c := v.Interface().(*tstCtx)
	assert.Equal(t, "mw1mw2mw3h1foo", c.String())
}

func TestRunHandlerError(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*middlewareHandler{
		{isGeneric: true, genericMiddleware: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		isGeneric:      false,
		dynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	v, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
	assert.Error(t, err)
	assert.Equal(t, "h1_err", err.Error())

	c := v.Interface().(*tstCtx)
	assert.Equal(t, "h1", c.String())
}

func TestRunMwError(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return fmt.Errorf("mw1_err")
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return fmt.Errorf("h1_err")
	}

	middleware := []*middlewareHandler{
		{isGeneric: true, genericMiddleware: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		isGeneric:      false,
		dynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
	assert.Error(t, err)
	assert.Equal(t, "mw1_err", err.Error())
}

func TestRunHandlerPanic(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		return next()
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")

		panic("dayam")
	}

	middleware := []*middlewareHandler{
		{isGeneric: true, genericMiddleware: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		isGeneric:      false,
		dynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

func TestRunMiddlewarePanic(t *testing.T) {
	mw1 := func(j *Job, next NextMiddlewareFunc) error {
		panic("dayam")
	}
	h1 := func(c *tstCtx, j *Job) error {
		c.record("h1")
		return nil
	}

	middleware := []*middlewareHandler{
		{isGeneric: true, genericMiddleware: mw1},
	}

	jt := &jobType{
		Name:           "foo",
		isGeneric:      false,
		dynamicHandler: reflect.ValueOf(h1),
	}

	job := &Job{
		Name: "foo",
	}

	_, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
	assert.Error(t, err)
	assert.Equal(t, "dayam", err.Error())
}

func TestRunGenericHandler(t *testing.T) {
	handlers := []struct {
		handler  interface{}
		mustFail bool
	}{
		{func(*Job) error { return nil }, false},
		{(&tstCtx{}).genericHandler, false},
		{(&tstCtx{}).genericContextHandler, false},
		{func(ctx context.Context, j *Job) error { return nil }, false},
		{GenericHandler(func(*Job) error { return nil }), true}, // default type is not casted
	}

	job := &Job{
		Name: "foo",
	}

	middleware := []*middlewareHandler{}

	for i, h := range handlers {
		jt := &jobType{
			Name:           "foo",
			isGeneric:      true,
			genericHandler: h.handler,
		}

		_, err := runJob(job, tstCtxType, middleware, jt, noopLogger)
		if !h.mustFail {
			assert.NoErrorf(t, err, "case: %d", i)
		} else {
			assert.Errorf(t, err, "case: %d", i)
		}
	}
}
