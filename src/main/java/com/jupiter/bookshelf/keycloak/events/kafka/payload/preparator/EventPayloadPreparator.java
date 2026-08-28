package com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator;

import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType;

public interface EventPayloadPreparator<T> {

    String prepare(PublishedEventType eventType, T source);
}
