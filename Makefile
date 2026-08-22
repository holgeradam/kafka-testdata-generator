.PHONY: test test-kafka

test:
	go test ./...

test-kafka:
	docker compose down -v 2>/dev/null || true
	docker compose up -d
	@echo "Waiting for Kafka to be ready (timeout: 30s)..."
	@timeout=30; \
	while [ "$$(docker inspect --format='{{.State.Health.Status}}' kafka-testdata 2>/dev/null)" != "healthy" ]; do \
		sleep 1; \
		timeout=$$((timeout - 1)); \
		if [ $$timeout -le 0 ]; then \
			echo "Error: Kafka failed to become ready"; \
			docker compose down -v; \
			exit 1; \
		fi; \
	done
	@echo "Kafka is ready. Creating topic..."
	@docker exec kafka-testdata /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --topic orders.created --partitions 1 --replication-factor 1 \
		--if-not-exists 2>/dev/null
	go run ./cmd/kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -count 5
	@echo ""
	@echo "Press Enter to shut down Kafka..."
	@read _dummy
	docker compose down -v
