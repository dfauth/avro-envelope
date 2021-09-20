package com.github.dfauth.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerde implements Serde<SpecificRecord> {

    private final Map<String, Object> config;
    private KafkaAvroDeserializer deserializer;
    private KafkaAvroSerializer serializer;

    public KafkaAvroSerde(SchemaRegistryClient schemaRegistryClient, String schemaRegistryUrl, Boolean isAutoRegisterSchema) {
        this.config = Map.of(
                "specific.avro.reader", true,
                "auto.register.schema", isAutoRegisterSchema,
                "schema.registry.url", schemaRegistryUrl
        );
        this.serializer = new KafkaAvroSerializer(schemaRegistryClient, config);
        this.deserializer = new KafkaAvroDeserializer(schemaRegistryClient, config);
    }

    @Override
    public Serializer<SpecificRecord> serializer() {
        return (t,o) -> serializer.serialize(t,o);
    }

    @Override
    public Deserializer<SpecificRecord> deserializer() {
        return (t,b) -> (SpecificRecord) deserializer.deserialize(t,b);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {
        serializer.close();
        deserializer.close();
    }
}
