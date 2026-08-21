# Kafka Testdata Generator

A CLI tool that reads an AsyncAPI specification, generates random test data conforming to the schema, and produces it to Kafka topics.

## Language

**AsyncAPI spec**:
A YAML or JSON document describing Kafka topics, their message schemas, and channel structure. The tool reads this to determine what data to generate.
_Avoid_: schema, contract, API spec

**Channel**:
A Kafka topic as described in the AsyncAPI spec. The tool produces to exactly one channel per run.
_Avoid_: topic (use "Kafka topic" when referring to the broker-level concept)

**Message schema**:
A JSON Schema embedded in the AsyncAPI spec's channel message payload. Defines the structure and constraints of generated test data.
_Aavoid_: payload schema

**Payload**:
A single generated test record conforming to the message schema. Serialized as JSON.
_Avoid_: message, record, event (use "payload" for the data, "message" for the Kafka envelope)

**Key**:
The Kafka message key, extracted from a field in the generated payload. Used for partitioning. Configurable field name via CLI.
_Avoid_: partition key

**Dry run**:
Mode where the tool generates payloads and prints them to stdout without producing to Kafka. Kafka-related flags are disregarded with a warning.
_Avoid_: console mode, stdout mode

**Count**:
Number of payloads to generate. Default is 10. Value of 0 means run indefinitely until interrupted.
_Avoid_: iterations, messages
