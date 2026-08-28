package com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry;

import com.jupiter.bookshelf.keycloak.events.kafka.strategy.EventPublicationStrategy;
import org.keycloak.events.EventType;

import java.util.Optional;

public interface EventPublicationStrategyRegistry {

    Optional<EventPublicationStrategy> get(EventType eventType);
}
