package com.devkafka.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class AvroDummyFiller {

    private static final Random RANDOM = new Random();

    public static GenericRecord generateDummyRecord(Schema schema) {
        GenericRecord genericRecord = new GenericData.Record(schema);

        for (Schema.Field field : schema.getFields()) {
            Object value = generateValue(field.schema());
            genericRecord.put(field.name(), value);
        }

        return genericRecord;
    }

    /**
     * Like {@link #generateDummyRecord(Schema)} but accepts any schema type,
     * not just RECORD (e.g. a topic key schema is often a primitive like
     * "string" rather than a record).
     */
    public static Object generateDummyValue(Schema schema) {
        return generateValue(schema);
    }

    private static Object generateValue(Schema fieldSchema) {
        switch (fieldSchema.getType()) {
            case STRING:
                return "dummy_" + RANDOM.nextInt(1000);
            case INT:
                return RANDOM.nextInt(100);
            case LONG:
                return RANDOM.nextLong() % 1000;
            case FLOAT:
                return RANDOM.nextFloat() * 100;
            case DOUBLE:
                return RANDOM.nextDouble() * 100;
            case BOOLEAN:
                return RANDOM.nextBoolean();

            case ENUM:
                String symbol;
                if (fieldSchema.getEnumDefault() != null) {
                    symbol = fieldSchema.getEnumDefault();
                } else {
                    List<String> symbols = fieldSchema.getEnumSymbols();
                    symbol = symbols.get(RANDOM.nextInt(symbols.size()));
                }
                return new GenericData.EnumSymbol(fieldSchema, symbol);

            case ARRAY:
                return List.of(Objects.requireNonNull(generateValue(fieldSchema.getElementType())));

            case MAP:
                return Map.of("key", Objects.requireNonNull(generateValue(fieldSchema.getValueType())));

            case RECORD:
                return generateDummyRecord(fieldSchema);

            case UNION:
                return fieldSchema.getTypes().stream()
                        .filter(t -> t.getType() != Schema.Type.NULL)
                        .findFirst()
                        .map(AvroDummyFiller::generateValue)
                        .orElse(null);

            case NULL:
                return null;

            default:
                return "unsupported_type_" + fieldSchema.getType();
        }
    }
}
