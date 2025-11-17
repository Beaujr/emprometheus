APP_NAME ?= emhass
PLATFORM ?= linux/amd64,linux/arm64
REGISTRY ?= docker.io
PUSH ?= true
TYPE ?= image
TAG ?= latest

releaser:
	rm -rf dist/
	goreleaser build --snapshot --id $(APP_NAME)
	mv dist/$(APP_NAME)_linux_amd64_v1 dist/$(APP_NAME)_linux_amd64
	mv dist/$(APP_NAME)_linux_arm64_v8.0 dist/$(APP_NAME)_linux_arm64

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

compose:
	docker compose -f docker-compose.yaml up -d