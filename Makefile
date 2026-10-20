APP_NAME ?= emhass
PLATFORM ?= linux/amd64,linux/arm64
REGISTRY ?= docker.io
PUSH ?= true
TYPE ?= image
TAG ?= latest
GOOS ?= linux
GOARCH ?= amd64
DB_URL ?= "postgres://postgres:postgres@192.168.1.92:5432/postgres?sslmode=disable"

build:
	GOOS=$(GOOS) GOARCH=$(GOARCH) CGO_ENABLED=0 go build  \
    		-a -tags netgo \
    		-o bin/${APP_NAME} \
    		./cmd/${APP_NAME}

releaser:
	rm -rf dist/
	goreleaser build --snapshot --id $(APP_NAME)
	mv dist/$(APP_NAME)_linux_amd64_v1 dist/$(APP_NAME)_linux_amd64
	mv dist/$(APP_NAME)_linux_arm64_v8.0 dist/$(APP_NAME)_linux_arm64

integration-test:
	docker compose -f docker-compose.yaml build promhass --no-cache
	@set -e; \
	trap 'docker compose -f docker-compose.yaml down' EXIT; \
	docker compose -f docker-compose.yaml up -d emhass postgres --wait; \
	docker compose -f docker-compose.yaml up dbmate promhass


docker:
		docker buildx build \
    		--build-arg APP_NAME=$(APP_NAME) \
    		--tag $(REGISTRY)/$(APP_NAME):$(TAG) \
    		--platform $(PLATFORM) \
    		--output "type=$(TYPE)" \
    		--file Dockerfile \
    		--push \
    		--provenance false \
    		./

compose: releaser
	docker compose -f docker-compose.yaml build
	docker compose -f docker-compose.yaml up -d

migrations:
	docker run --rm \
      -v "${shell pwd}/db:/db" \
      -e DATABASE_URL=$(DB_URL) \
      amacneil/dbmate up

sqlc:
	docker run --rm -v "${shell pwd}:/src" -w /src sqlc/sqlc generate

postgres:
	docker run -d --rm --name postgres \
    -p 5432:5432 \
    -e POSTGRES_PASSWORD="postgres" \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    --memory=4g --memory-swap=4g \
    postgres