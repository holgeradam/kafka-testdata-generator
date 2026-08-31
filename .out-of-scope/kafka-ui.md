# Kafka UI

This project does not provide a web UI for inspecting topics and messages.

## Why this is out of scope

This is a small, simple test-data generator. Its job is to read an AsyncAPI
specification, synthesize conforming JSON payloads, and produce them to a Kafka
channel (or print them during a dry run). A visual UI for inspecting topics and
messages is a separate, much larger product concern - it implies a web server,
persistent state, streaming/consumption tooling, and a frontend. None of that
belongs to a focused CLI that generates test data.

Anyone wanting to inspect Kafka topics already has purpose-built tools (Kafka
UI, Kowl, AKHQ, kcat, etc.), so building one here would duplicate existing
ecosystem tooling rather than add value to this project.

## Prior requests

- #5: "Decide fate of 'Kafka UI' bullet added to README Future Features"
