package com.jupiter.bookshelf.keycloak.events.kafka.listener.provider.factory;

import com.jupiter.bookshelf.keycloak.events.kafka.config.ListenerConfig;
import com.jupiter.bookshelf.keycloak.events.kafka.holder.KafkaProducerHolder;
import com.jupiter.bookshelf.keycloak.events.kafka.listener.provider.KafkaEventListenerProvider;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator.UserEventPayloadPreparator;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.EventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.UserLoggedInEventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.UserRegisteredEventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry.EventPublicationStrategyRegistry;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry.EventPublicationStrategyRegistryImpl;
import org.keycloak.Config;
import org.keycloak.events.EventListenerProvider;
import org.keycloak.events.EventListenerProviderFactory;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.KeycloakSessionFactory;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.List;

public class KafkaEventListenerProviderFactory implements EventListenerProviderFactory {

    private static final String PROVIDER_ID = "bookshelf-kafka";

    private static final Logger LOG = System.getLogger(KafkaEventListenerProviderFactory.class.getName());

    private volatile ListenerConfig config;
    private volatile KafkaProducerHolder producerHolder;
    private volatile EventPublicationStrategyRegistry strategyRegistry;

    @Override
    public EventListenerProvider create(KeycloakSession session) {
        return new KafkaEventListenerProvider(session, producerHolder, strategyRegistry, config);
    }

    @Override
    public void init(Config.Scope scope) {
        this.config = ListenerConfig.from(scope);
    }

    @Override
    public void postInit(KeycloakSessionFactory factory) {
        if (config == null) {
            config = ListenerConfig.from(null);
        }
        this.producerHolder = new KafkaProducerHolder(config);
        this.strategyRegistry = buildStrategyRegistry(config);
        LOG.log(Level.INFO, "bookshelf-kafka event listener ready: topic={0}, bootstrap={1}, emitLogin={2}",
            config.topic(), config.bootstrapServers(), config.emitLogin());
    }

    @Override
    public void close() {
        if (producerHolder != null) {
            producerHolder.close();
        }
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    private static EventPublicationStrategyRegistry buildStrategyRegistry(ListenerConfig config) {
        var payloadPreparator = new UserEventPayloadPreparator();
        List<EventPublicationStrategy> strategies = List.of(
            new UserRegisteredEventPublicationStrategy(payloadPreparator),
            new UserLoggedInEventPublicationStrategy(payloadPreparator, config)
        );
        return new EventPublicationStrategyRegistryImpl(strategies);
    }
}
