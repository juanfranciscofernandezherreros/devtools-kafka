package com.devkafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AvroDummyFillerTest {

    private static final String SIMPLE_SCHEMA_JSON = """
            {
              "type": "record",
              "name": "User",
              "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "active", "type": "boolean"}
              ]
            }
            """;

    @Test
    @DisplayName("Generate simple dummy record")
    void testGenerateDummyRecord_simpleSchema() {
        Schema schema = new Schema.Parser().parse(SIMPLE_SCHEMA_JSON);
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        assertNotNull(record, "The record must not be null");
        assertEquals(schema, record.getSchema(), "The schema must match");

        System.out.println("🧩 Generated record: " + record);

        // Check field types
        assertTrue(record.get("id") instanceof Integer);
        assertTrue(record.get("name") instanceof String);
        assertTrue(record.get("active") instanceof Boolean);
    }

    @Test
    @DisplayName("Generate record with complex types (array, union, nested record)")
    void testGenerateDummyRecord_complexSchema() {
        String complexSchemaJson = """
                {
                  "type": "record",
                  "name": "Order",
                  "fields": [
                    {"name": "orderId", "type": "long"},
                    {"name": "items", "type": {"type": "array", "items": "string"}},
                    {"name": "metadata", "type": {"type": "map", "values": "string"}},
                    {"name": "customer", "type": {
                      "type": "record",
                      "name": "Customer",
                      "fields": [
                        {"name": "id", "type": "int"},
                        {"name": "email", "type": ["null", "string"], "default": null}
                      ]
                    }}
                  ]
                }
                """;

        Schema schema = new Schema.Parser().parse(complexSchemaJson);
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        assertNotNull(record, "The generated record must not be null");
        assertNotNull(record.get("customer"), "It must have a 'customer' subrecord");
        assertTrue(record.get("items") instanceof java.util.List);
        assertTrue(record.get("metadata") instanceof java.util.Map);

        System.out.println("📦 Generated complex record: " + record);
    }

    @Test
    @DisplayName("ENUM and NULL fields are filled correctly")
    void testEnumAndNull() {
        String schemaJson = """
                {
                  "type": "record",
                  "name": "StatusTest",
                  "fields": [
                    {"name": "status", "type": {"type": "enum", "name": "StatusEnum", "symbols": ["OK", "FAIL"]}},
                    {"name": "optionalField", "type": ["null", "string"], "default": null}
                  ]
                }
                """;

        Schema schema = new Schema.Parser().parse(schemaJson);
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        Object status = record.get("status");
        assertTrue(status.toString().equals("OK") || status.toString().equals("FAIL"), "Status must be one of the valid enum symbols");
        assertTrue(record.get("optionalField") == null || record.get("optionalField") instanceof String);
    }
}
