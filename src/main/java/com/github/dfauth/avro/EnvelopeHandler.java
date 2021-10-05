package com.github.dfauth.avro;

import com.google.common.collect.ImmutableMap;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class EnvelopeHandler<T extends SpecificRecord> {

    private static final Logger logger = LoggerFactory.getLogger(EnvelopeHandler.class);

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public static <T extends SpecificRecord, R extends SpecificRecord> EnvelopeHandler<R> recast(EnvelopeHandler<T> envelopeHandler) {
        return EnvelopeHandler.of(new AvroSerialization(new Serde(){
            @Override
            public Serializer<SpecificRecord> serializer() {
                return (Serializer<SpecificRecord>) envelopeHandler.serializer;
            }

            @Override
            public Deserializer<SpecificRecord> deserializer() {
                return (Deserializer<SpecificRecord>) envelopeHandler.deserializer;
            }
        }));
    }

    public static <T extends SpecificRecord> EnvelopeHandler<T> of(AvroSerialization avroSerialization, Class<T> targetClass) {
        return new EnvelopeHandler<>(
                avroSerialization.serializer(targetClass),
                avroSerialization.deserializer(targetClass));
    }

    public static <T extends SpecificRecord> EnvelopeHandler<T> of(AvroSerialization avroSerialization) {
        return new EnvelopeHandler<>(
                avroSerialization.serializer(),
                avroSerialization.deserializer());
    }

    public EnvelopeHandler(Serializer<T> serializer, Deserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    public Envelope envelope(final T t) {
        return envelope(t, Collections.emptyMap());
    }

    public Envelope envelope(final T t, String id) {
        return envelope(t, id, Collections.emptyMap());
    }

    public Envelope envelope(final T t, final Map<String, String> metadata) {
        return envelope(t, UUID.randomUUID().toString(), metadata);
    }

    public Envelope envelope(final T t, String id, final Map<String, String> metadata) {
        requireNonNull(t, "schema is null");

        final String canonicalName = t.getClass().getCanonicalName();
        try {
            logger.info("Creating payload with id [{}], schemaRegistryTopic [{}]", id, canonicalName);
            final byte[] serialize = serializer.serialize(canonicalName, t);

            return Envelope.newBuilder()
                    .setId(id)
                    .setPayload(ByteBuffer.wrap(serialize))
                    .setMetadata(metadata != null ? metadata : ImmutableMap.of())
                    .setSchemaRegistryTopic(canonicalName)
                    .build();
        } catch (Exception e) {
            final String schema = t.getSchema().toString(true);
            final String exMsg = "Unable to create envelope for specificRecord for canonicalName: [" + canonicalName + "] and with schema [" + schema + "]";
            logger.error(exMsg, e);
            throw new SerializationException(exMsg, e);
        }
    }

    public T extractRecord(Envelope e) {
        requireNonNull(e, "envelope is null");
        requireNonNull(e.getPayload(), "envelope payload is null");

        try {
            logger.info("Deserialize payload with id=[{}], schemaRegistryTopic=[{}]", e.getId(), e.getSchemaRegistryTopic());
            return deserializer.deserialize(e.getSchemaRegistryTopic(), e.getPayload().array());
        } catch (Exception e1) {
            final String message = "Unable to extract record from envelope with topic " + e.getSchemaRegistryTopic();
            logger.error(message, e);
            throw new SerializationException(message, e1);
        }
    }

    public <R> R extractRecord(Envelope e, BiFunction<T, Map<String,String>,R> f) {
        requireNonNull(e, "envelope is null");
        requireNonNull(e.getPayload(), "envelope payload is null");
        requireNonNull(f, "envelope processor is null");

        try {
            logger.info("Deserialize payload with id=[{}], schemaRegistryTopic=[{}]", e.getId(), e.getSchemaRegistryTopic());
            return f.apply(deserializer.deserialize(e.getSchemaRegistryTopic(), e.getPayload().array()),e.getMetadata());
        } catch (Exception e1) {
            final String message = "Unable to extract record from envelope with topic " + e.getSchemaRegistryTopic();
            logger.error(message, e);
            throw new SerializationException(message, e1);
        }
    }
}
