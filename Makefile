run-redis:
	docker run \
		--name work-redis \
		--rm \
		-p 6379:6379 \
		-d \
		--mount type=bind,source=${PWD},target=/tmp/work \
		redis:6.2.6

stop-redis:
	docker stop work-redis

run-tests:
	-go test -v -vet all -cover -race -p 1 ./...

test: run-redis run-tests stop-redis

.PHONY: run-redis stop-redis run-tests test
