package com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.KafkaEventEnvelope;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType;
import org.keycloak.util.JsonSerialization;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class AbstractEventPayloadPreparator<T> implements EventPayloadPreparator<T> {

    private final ObjectMapper mapper;

    protected AbstractEventPayloadPreparator() {
        this(JsonSerialization.mapper);
    }

    protected AbstractEventPayloadPreparator(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    @Override
    public final String prepare(PublishedEventType eventType, T source) {
        return write(new KafkaEventEnvelope(
            eventType.getValue(),
            write(toPayload(source))
        ));
    }

    /** Map the domain object to the record that becomes the inner {@code payload} JSON. */
    protected abstract Object toPayload(T source);

    private String write(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (IOException e) {
            throw new UncheckedIOException("bookshelf-kafka: failed to serialise event payload", e);
        }
    }
}
