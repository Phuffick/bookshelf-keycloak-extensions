package com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry;

import com.jupiter.bookshelf.keycloak.events.kafka.strategy.EventPublicationStrategy;
import org.junit.jupiter.api.Test;
import org.keycloak.events.EventType;
import org.keycloak.models.UserModel;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EventPublicationStrategyRegistryImplTest {

    private final EventPublicationStrategy register = strategy(EventType.REGISTER);
    private final EventPublicationStrategy login = strategy(EventType.LOGIN);

    @Test
    void get_returnsTheStrategyRegisteredForTheEventType() {
        var registry = new EventPublicationStrategyRegistryImpl(List.of(register, login));

        assertThat(registry.get(EventType.REGISTER)).containsSame(register);
        assertThat(registry.get(EventType.LOGIN)).containsSame(login);
    }

    @Test
    void get_returnsEmptyForAnUnregisteredEventType() {
        var registry = new EventPublicationStrategyRegistryImpl(List.of(register));

        assertThat(registry.get(EventType.LOGOUT)).isEmpty();
    }

    @Test
    void constructor_rejectsTwoStrategiesForTheSameEventType() {
        assertThatThrownBy(() -> new EventPublicationStrategyRegistryImpl(List.of(register, strategy(EventType.REGISTER))))
            .isInstanceOf(IllegalStateException.class);
    }

    private static EventPublicationStrategy strategy(EventType eventType) {
        return new EventPublicationStrategy() {
            @Override
            public EventType getApplicableEventType() {
                return eventType;
            }

            @Override
            public boolean isEnabled() {
                return true;
            }

            @Override
            public String prepareMessage(UserModel user) {
                return "";
            }
        };
    }
}
