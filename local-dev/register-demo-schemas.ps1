# Registers the Avro schemas used by demo-app/samples/*.json against the
# local Schema Registry (local-dev/docker-compose.yml), under the subjects
# GenericRunnerBean.run(...) will look up: <topic>-key and <topic>-value.
#
# Usage: powershell -File local-dev/register-demo-schemas.ps1 [schema-registry-url]
param(
    [string]$SchemaRegistryUrl = "http://localhost:8081"
)

$ErrorActionPreference = "Stop"

function Register-Schema([string]$Subject, [string]$Schema) {
    $body = @{ schema = $Schema } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method Post `
        -Uri "$SchemaRegistryUrl/subjects/$Subject/versions" `
        -ContentType "application/vnd.schemaregistry.v1+json" `
        -Body $body | Out-Null
    Write-Host "Registered $Subject"
}

Register-Schema -Subject "test-topic-key" -Schema '{"type":"string"}'
Register-Schema -Subject "test-topic-value" -Schema '{"type":"record","name":"DemoValue","namespace":"com.devkafka.demo","fields":[{"name":"id","type":"int"},{"name":"message","type":"string"}]}'
