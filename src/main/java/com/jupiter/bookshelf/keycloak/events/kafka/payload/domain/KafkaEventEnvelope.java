package com.jupiter.bookshelf.keycloak.events.kafka.payload.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record KafkaEventEnvelope(
    String eventType,
    String payload
) {
}
