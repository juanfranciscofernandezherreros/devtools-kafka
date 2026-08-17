#!/usr/bin/env bash
# Registers the Avro schemas used by demo-app/samples/*.json against the
# local Schema Registry (local-dev/docker-compose.yml), under the subjects
# GenericRunnerBean.run(...) will look up: <topic>-key and <topic>-value.
#
# Usage: local-dev/register-demo-schemas.sh [schema-registry-url]
set -euo pipefail

SR_URL="${1:-http://localhost:8081}"

curl -sf -X POST "$SR_URL/subjects/test-topic-key/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"string\"}"}' \
  && echo "" && echo "Registered test-topic-key"

curl -sf -X POST "$SR_URL/subjects/test-topic-value/versions" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d '{"schema": "{\"type\":\"record\",\"name\":\"DemoValue\",\"namespace\":\"com.devkafka.demo\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"message\",\"type\":\"string\"}]}"}' \
  && echo "" && echo "Registered test-topic-value"
