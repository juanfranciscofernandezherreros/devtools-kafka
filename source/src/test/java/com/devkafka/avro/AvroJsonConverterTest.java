package com.devkafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class AvroJsonConverterTest {

    private static final String USER_SCHEMA_JSON = """
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
    @DisplayName("Converts a valid Avro record to JSON correctly")
    void testToJson_validRecord() throws IOException {
        // Parse the schema
        Schema schema = new Schema.Parser().parse(USER_SCHEMA_JSON);

        // Generate a dummy example record
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        // Convert to JSON
        String json = AvroJsonConverter.toJson(record, schema);

        System.out.println("🧩 Generated JSON:\n" + json);

        // Basic validations
        assertNotNull(json, "JSON must not be null");
        assertTrue(json.contains("id"), "It must contain the 'id' field");
        assertTrue(json.contains("name"), "It must contain the 'name' field");
        assertTrue(json.contains("active"), "It must contain the 'active' field");
    }

    @Test
    @DisplayName("Throws exception if the record or schema are null")
    void testToJson_nullArguments() {
        Schema schema = new Schema.Parser().parse(USER_SCHEMA_JSON);
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        // Case 1: null record
        Exception ex1 = assertThrows(IllegalArgumentException.class,
                () -> AvroJsonConverter.toJson(null, schema));
        assertTrue(ex1.getMessage().contains("Record and schema cannot be null"));

        // Case 2: null schema
        Exception ex2 = assertThrows(IllegalArgumentException.class,
                () -> AvroJsonConverter.toJson(record, null));
        assertTrue(ex2.getMessage().contains("Record and schema cannot be null"));
    }

    @Test
    @DisplayName("The generated JSON must be valid and readable (not empty)")
    void testJsonOutputIsReadable() throws IOException {
        Schema schema = new Schema.Parser().parse(USER_SCHEMA_JSON);
        GenericRecord record = AvroDummyFiller.generateDummyRecord(schema);

        String json = AvroJsonConverter.toJson(record, schema);
        assertFalse(json.isBlank(), "JSON must not be empty");
        assertTrue(json.startsWith("{"), "JSON must start with '{'");
        assertTrue(json.endsWith("}"), "JSON must end with '}'");
    }
}
