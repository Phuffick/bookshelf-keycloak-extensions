package com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry;

import com.jupiter.bookshelf.keycloak.events.kafka.strategy.EventPublicationStrategy;
import org.keycloak.events.EventType;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EventPublicationStrategyRegistryImpl implements EventPublicationStrategyRegistry {

    private final Map<EventType, EventPublicationStrategy> strategiesByEventType;

    public EventPublicationStrategyRegistryImpl(List<EventPublicationStrategy> strategies) {
        this.strategiesByEventType = strategies.stream()
            .collect(Collectors.toMap(EventPublicationStrategy::getApplicableEventType, Function.identity()));
    }

    @Override
    public Optional<EventPublicationStrategy> get(EventType eventType) {
        return Optional.ofNullable(strategiesByEventType.get(eventType));
    }
}
