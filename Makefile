.PHONY: test test-kafka

# Detect container runtime (podman or docker)
CONTAINER_RUNTIME := $(shell if command -v podman > /dev/null 2>&1; then echo podman; elif command -v docker > /dev/null 2>&1; then echo docker; else echo ""; fi)
ifeq ($(CONTAINER_RUNTIME),)
$(error No container runtime found. Install docker or podman.)
endif

test:
	go test ./...

test-kafka:
	@echo "Using container runtime: $(CONTAINER_RUNTIME)"
	$(CONTAINER_RUNTIME) compose down -v 2>/dev/null || true
	$(CONTAINER_RUNTIME) compose up -d
	@echo "Waiting for Kafka to be ready (timeout: 30s)..."
	@timeout=30; \
	while [ "$$($(CONTAINER_RUNTIME) inspect --format='{{.State.Health.Status}}' kafka-testdata 2>/dev/null)" != "healthy" ]; do \
		sleep 1; \
		timeout=$$((timeout - 1)); \
		if [ $$timeout -le 0 ]; then \
			echo "Error: Kafka failed to become ready"; \
			$(CONTAINER_RUNTIME) compose down -v; \
			exit 1; \
		fi; \
	done
	@echo "Kafka is ready. Creating topic..."
	@$(CONTAINER_RUNTIME) exec kafka-testdata /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server localhost:9092 \
		--create --topic orders.created --partitions 1 --replication-factor 1 \
		--if-not-exists 2>/dev/null
	go run ./cmd/kafka-testdata-generator -spec examples/order.asyncapi.yaml -channel orders.created -count 5
	@echo ""
	@echo "Press Enter to shut down Kafka..."
	@read _dummy
	$(CONTAINER_RUNTIME) compose down -v
