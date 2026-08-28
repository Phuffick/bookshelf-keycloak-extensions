package com.jupiter.bookshelf.keycloak.events.kafka.strategy;

import com.jupiter.bookshelf.keycloak.events.kafka.config.ListenerConfig;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator.EventPayloadPreparator;
import org.keycloak.events.EventType;
import org.keycloak.models.UserModel;

public class UserLoggedInEventPublicationStrategy implements EventPublicationStrategy {

    private final EventPayloadPreparator<UserModel> payloadPreparator;
    private final ListenerConfig config;

    public UserLoggedInEventPublicationStrategy(EventPayloadPreparator<UserModel> payloadPreparator,
                                                ListenerConfig config) {
        this.payloadPreparator = payloadPreparator;
        this.config = config;
    }

    @Override
    public EventType getApplicableEventType() {
        return EventType.LOGIN;
    }

    @Override
    public boolean isEnabled() {
        return config.emitLogin();
    }

    @Override
    public String prepareMessage(UserModel user) {
        return payloadPreparator.prepare(PublishedEventType.USER_LOGGED_IN_EVENT, user);
    }
}
