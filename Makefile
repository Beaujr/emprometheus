APP_NAME ?= emhass
PLATFORM ?= linux/amd64,linux/arm64
REGISTRY ?= docker.io
PUSH ?= true
TYPE ?= image
TAG ?= latest
GOOS ?= linux
GOARCH ?= amd64


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
	@set -e; \
	trap 'docker compose -f docker-compose.yaml down' EXIT; \
	docker compose -f docker-compose.yaml up -d emhass --wait; \
	docker compose -f docker-compose.yaml up promhass


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