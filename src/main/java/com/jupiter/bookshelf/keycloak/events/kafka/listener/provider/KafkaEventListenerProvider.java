package com.jupiter.bookshelf.keycloak.events.kafka.listener.provider;

import com.jupiter.bookshelf.keycloak.events.kafka.config.ListenerConfig;
import com.jupiter.bookshelf.keycloak.events.kafka.holder.KafkaProducerHolder;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry.EventPublicationStrategyRegistry;
import org.keycloak.events.Event;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventType;
import org.keycloak.events.admin.AdminEvent;
import org.keycloak.models.KeycloakSession;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.UUID;

public class KafkaEventListenerProvider implements EventListenerProvider {

    private static final Logger LOG = System.getLogger(KafkaEventListenerProvider.class.getName());

    private final KeycloakSession session;
    private final KafkaProducerHolder producerHolder;
    private final EventPublicationStrategyRegistry strategyRegistry;
    private final ListenerConfig config;

    public KafkaEventListenerProvider(KeycloakSession session,
                                      KafkaProducerHolder producerHolder,
                                      EventPublicationStrategyRegistry strategyRegistry,
                                      ListenerConfig config) {
        this.session = session;
        this.producerHolder = producerHolder;
        this.strategyRegistry = strategyRegistry;
        this.config = config;
    }

    @Override
    public void onEvent(Event event) {
        try {
            onEventThrowing(event);
        } catch (RuntimeException e) {
            LOG.log(Level.ERROR, "bookshelf-kafka: failed to handle Keycloak event", e);
        }
    }

    @Override
    public void onEvent(AdminEvent adminEvent, boolean includeRepresentation) {
        // No-op: admin-console/API-created users fire admin events, not REGISTER - out of scope for now.
    }

    @Override
    public void close() {
        // The producer is owned and closed by the factory.
    }

    private void onEventThrowing(Event event) {
        var strategy = strategyRegistry.get(event.getType())
            .orElse(null);
        if (strategy == null || !strategy.isEnabled()) {
            return;
        }
        var realm = session.realms()
            .getRealm(event.getRealmId());
        if (realm == null || !config.realm().equals(realm.getName())) {
            return;
        }
        var userId = event.getUserId();
        if (userId == null || userId.isBlank()) {
            LOG.log(Level.WARNING, "bookshelf-kafka: {0} event has no userId, skipped", event.getType());
            return;
        }
        var user = session.users()
            .getUserById(realm, userId);
        if (user == null) {
            LOG.log(Level.WARNING, "bookshelf-kafka: no user {0} for {1} event, skipped", userId, event.getType());
            return;
        }
        producerHolder.publish(
            userId,
            strategy.prepareMessage(user),
            UUID.randomUUID().toString()
        );
    }
}
