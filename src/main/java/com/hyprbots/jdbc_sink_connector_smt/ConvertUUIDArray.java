package com.hyprbots.jdbc_sink_connector_smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class ConvertUUIDArray<R extends ConnectRecord<R>> implements Transformation<R> {
    private Cache<Schema, Schema> schemaUpdateCache;
    public static final String PURPOSE ="convert array of strings to array of UUID";
    public static final String FIELD_CONFIG = "field";
    public static final String FIELD_DEFAULT = "";
    String field;

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return record;
        } else {
            return applyWithSchema(record);
        }
    }

    private R applyWithSchema(R record) {
        final Struct struct = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(struct.schema());
//        if(updatedSchema == null) {
//            updatedSchema = makeUpdatedSchema(struct.schema());
//            schemaUpdateCache.put(struct.schema(), updatedSchema);
//        }

        final Struct updatedValue = new Struct(updatedSchema);
        Field fieldSchema = updatedSchema.field(field);

        if (fieldSchema.schema().type() == Schema.Type.ARRAY && fieldSchema.schema().name() != null && fieldSchema.schema().schema().type() == Schema.Type.STRING) {
            List<String> uuidStrings = (List<String>) updatedValue.get(fieldSchema.name());
            UUID[] uuids = uuidStrings.stream().map(UUID::fromString).toArray(UUID[]::new);
            updatedValue.put(fieldSchema.name(), uuids);
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef().define(FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.HIGH,
                "The field containing the array of strings to be converted to an array of UUID");
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig simpleConfig = new SimpleConfig(new ConfigDef(), configs);
        field = simpleConfig.getString(FIELD_CONFIG);
        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));

    }

    private Schema operatingSchema(R record) {
        return record.valueSchema();
    }

    protected Object operatingValue(R record) {
        return record.value();
    }

    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                updatedSchema,
                updatedValue,
                record.timestamp()
        );
    }

}

