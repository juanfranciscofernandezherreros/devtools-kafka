package com.devkafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * Utility to convert an Avro GenericRecord into a valid JSON string.
 *
 * Normally used after generating dummy records with AvroDummyFiller
 * or after deserializing Avro data for inspection.
 */
public class AvroJsonConverter {

    /**
     * Converts a generic Avro record to a readable JSON string.
     *
     * @param record record the generic Avro record to convert
     * @param schema schema the Avro schema corresponding to the record
     * @return a JSON string with the record's content
     * @throws IOException if serialization fails
     */
    public static String toJson(GenericRecord record, Schema schema) throws IOException {
        if (record == null || schema == null) {
            throw new IllegalArgumentException("Record and schema cannot be null");
        }

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
            Encoder encoder = EncoderFactory.get().jsonEncoder(schema, out, true);
            writer.write(record, encoder);
            encoder.flush();
            return out.toString();
        }
    }
}
