# SlateDB WAL benchmark

This crate runs a SlateDB writer and a `DbReader` that tails the same WAL. It
supports the Kafka WAL and the native object-store WAL. The workload targets
1,000 writes per second and, every 100 ms, waits for the latest write to become
durable, waits for the reader to expose its sequence number, verifies the
value, and records the latency between the two status transitions.

Every run creates and retains a database under
`s3://opendata-vector-bench/wal-rfc-proto/<run-id>`. Kafka runs also create and
retain a one-partition topic named `slatedb-kafka-wal-example-<run-id>`.

## Run

Kafka is the default backend. It reads MSK IAM settings from
`client.properties` in the current directory:

```bash
WAL_BACKEND=kafka \
TEST_DURATION_SECONDS=60 \
cargo run --release -p slatedb-kafka-wal-example
```

For lower Kafka WAL-tail latency, add `fetch.wait.max.ms=10` to the properties
file. Lower values poll an idle tail more often and therefore increase empty
fetch requests and broker/client work.

Run the native object-store WAL with:

```bash
WAL_BACKEND=object_store \
TEST_DURATION_SECONDS=60 \
cargo run --release -p slatedb-kafka-wal-example
```

Object-store mode does not read Kafka properties or create a Kafka topic. Its
native WAL tailer polls S3 every 10 ms.

`TEST_DURATION_SECONDS` defaults to `60`. Optional settings are:

- `WAL_BACKEND`: `kafka` or `object_store`; defaults to `kafka`.
- `KAFKA_CLIENT_PROPERTIES`: properties file path; defaults to `client.properties`.
- `KAFKA_COMMIT_INTERVAL_MS`: Kafka transaction commit interval; defaults to `100`.
- `KAFKA_TOPIC_PREFIX`: per-run topic prefix; defaults to `slatedb-kafka-wal-example`.
- `KAFKA_DEBUG`: librdkafka debug facilities such as `security,broker,protocol`.

The AWS default credential chain must be authorized for the S3 prefix. Kafka
mode additionally requires MSK IAM connection, transactional write, read,
topic description, and topic creation permissions. Java-specific
`AWS_MSK_IAM` properties are translated to the librdkafka `OAUTHBEARER`
mechanism and signed with the same AWS credentials.

The final report includes the achieved write rate, verified sample count, and
average, p50, p90, and p99 durable-to-readable latency.
