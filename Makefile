.DEFAULT_GOAL: help

.PHONY: help build modules setup format

GO              ?= go
MAKEFILE_PATH   := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
APP				:= app

help: ## Displays this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

setup:
	$(GO) mod init github.com/AClarkie/sumo-log-exporter
	make modules
	make format

modules: ## Tidy up and update vendor dependencies
	$(GO) mod tidy
	$(GO) mod vendor

format:
	$(GO) fmt $$($(GO) list ./...)

build: ## Builds the app
	echo "To build $(APP) binary"
	$(GO) build -o $(APP) ./cmd
