package com.github.dfauth.avro;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class AvroSerialization {

    private final Serde<SpecificRecord> serde;

    public AvroSerialization(SchemaRegistryClient schemaRegistryClient, String schemaRegistryUrl, Boolean isAutoRegisterSchema) {
        this(new KafkaAvroSerde(schemaRegistryClient, schemaRegistryUrl, isAutoRegisterSchema));
    }

    public AvroSerialization(Serde<SpecificRecord> serde) {
        this.serde = serde;
    }

    public Serializer<Envelope> envelopeSerializer() {
        return (t,o) -> serde.serializer().serialize(t,o);
    }

    public Deserializer<Envelope> envelopeDeserializer() {
        return (t,b) -> (Envelope) serde.deserializer().deserialize(t,b);
    }

    public <T extends SpecificRecord> Serializer<T> serializer() {
        return (t,o) -> serde.serializer().serialize(t,o);
    }

    public <T extends SpecificRecord> Deserializer<T> deserializer() {
        return (t,b) -> (T) serde.deserializer().deserialize(t,b);
    }

    public <T extends SpecificRecord> Serializer<T> serializer(Class<T> targetClass) {
        return (t,o) -> serde.serializer().serialize(t,o);
    }

    public <T extends SpecificRecord> Deserializer<T> deserializer(Class<T> targetClass) {
        return (t,b) -> (T) serde.deserializer().deserialize(t,b);
    }
}
