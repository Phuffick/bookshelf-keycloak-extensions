package com.jupiter.bookshelf.keycloak.events.kafka.strategy;

import org.keycloak.events.EventType;
import org.keycloak.models.UserModel;

public interface EventPublicationStrategy {

    EventType getApplicableEventType();

    boolean isEnabled();

    String prepareMessage(UserModel user);
}
