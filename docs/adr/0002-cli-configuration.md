# CLI configuration with sensible defaults

All configuration is via CLI flags with sensible defaults. Environment variable support is a future feature. The broker address defaults to `localhost:9092`. The channel (Kafka topic) is required. Count defaults to 10, rate limit to 10ms between payloads. The key field defaults to `id` but can be overridden. If no key field is found in the generated payload, the tool warns and exits.

This keeps the tool simple and predictable for the common case while remaining configurable.
