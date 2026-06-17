.PHONY: install lint build run compose-up compose-down logs test-compose

install:
	npm install

lint:
	npx spectral lint contracts/*.yaml

build:
	docker build -t fit4110/iot-ingestion:v0.1.0-team-iot .

run:
	docker run --rm --name fit4110-api-lab05 -p 8000:8000 --env-file .env.example fit4110/iot-ingestion:v0.1.0-team-iot

compose-up:
	docker compose up -d --build --wait

compose-down:
	docker compose down -v

logs:
	docker compose logs -f

test-compose:
	mkdir -p reports
	npm run test:compose
