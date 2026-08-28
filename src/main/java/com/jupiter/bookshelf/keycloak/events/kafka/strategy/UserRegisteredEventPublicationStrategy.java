package com.jupiter.bookshelf.keycloak.events.kafka.strategy;

import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator.EventPayloadPreparator;
import org.keycloak.events.EventType;
import org.keycloak.models.UserModel;

public class UserRegisteredEventPublicationStrategy implements EventPublicationStrategy {

    private final EventPayloadPreparator<UserModel> payloadPreparator;

    public UserRegisteredEventPublicationStrategy(EventPayloadPreparator<UserModel> payloadPreparator) {
        this.payloadPreparator = payloadPreparator;
    }

    @Override
    public EventType getApplicableEventType() {
        return EventType.REGISTER;
    }

    @Override
    public boolean isEnabled() {
        return true;
    }

    @Override
    public String prepareMessage(UserModel user) {
        return payloadPreparator.prepare(PublishedEventType.USER_REGISTERED_EVENT, user);
    }
}
