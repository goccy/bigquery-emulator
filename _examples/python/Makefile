.PHONY: setup
setup: build up

.PHONY: build
build:
	docker-compose build

.PHONY: up
up:
	docker-compose up -d bigquery-emulator

.PHONY: run
run:
	docker-compose run app
