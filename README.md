# kafka-testdata-generator

Generates random, realistic-looking JSON test records from AsyncAPI message payload schemas.

## Usage

```sh
go run ./cmd/kafka-testdata-generator \
  -spec examples/order.asyncapi.yaml \
  -channel orders.created \
  -count 3
```

Records are written as newline-delimited JSON to stdout. A short generation summary is written to stderr.

Useful flags:

- `-spec`: path to the AsyncAPI document, in YAML or JSON format
- `-channel`: channel to generate from; defaults to the first channel
- `-message`: message name, useful when a channel uses `oneOf`
- `-count`: number of records to generate
- `-seed`: deterministic random seed; the same seed and spec produce the same records
- `-pretty`: pretty-print each JSON record

## Supported schema features

The first version focuses on the subset commonly used in AsyncAPI payloads:

- `type`: `object`, `array`, `string`, `integer`, `number`, and `boolean`
- `properties`, `required`, `items`, `minItems`, `maxItems`
- `enum`, `const`, `example`, and `examples`
- string formats: `date-time`, `date`, `email`, `uuid`, `uri`, and `url`
- numeric bounds: `minimum`, `maximum`, `exclusiveMinimum`, and `exclusiveMaximum`
- internal `$ref` references for messages and payloads
- channel messages via `publish`/`subscribe` or channel-level `messages`
- `allOf`, `oneOf`, and `anyOf`

Generated values use schema hints plus field-name heuristics, so fields such as `email`, `customerId`, `totalAmount`, `city`, and `status` produce data that is random but plausible.
