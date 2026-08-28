package com.jupiter.bookshelf.keycloak.events.kafka.config;

import org.keycloak.Config;

import java.util.Optional;
import java.util.function.Predicate;

/**
 * Configuration for the {@code bookshelf-kafka} event listener. Each value is read from the Keycloak
 * SPI config first (e.g. {@code --spi-events-listener-bookshelf-kafka-bootstrap-servers=...} or
 * {@code KC_SPI_EVENTS_LISTENER_BOOKSHELF_KAFKA_BOOTSTRAP_SERVERS}), then from a plain environment
 * variable, then a default.
 */
public record ListenerConfig(
    String bootstrapServers,
    String topic,
    String clientId,
    String realm,
    boolean emitLogin
) {

    public static ListenerConfig from(Config.Scope scope) {
        return new ListenerConfig(
            resolve(scope, "bootstrapServers", "BOOKSHELF_KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
            resolve(scope, "topic", "BOOKSHELF_KAFKA_TOPIC", "user-events"),
            resolve(scope, "clientId", "BOOKSHELF_KAFKA_CLIENT_ID", "keycloak-bookshelf"),
            resolve(scope, "realm", "BOOKSHELF_KAFKA_REALM", "bookshelf"),
            Boolean.parseBoolean(resolve(scope, "emitLogin", "BOOKSHELF_KAFKA_EMIT_LOGIN", "true"))
        );
    }

    private static String resolve(Config.Scope scope,
                                  String key,
                                  String env,
                                  String fallback) {
        return Optional.ofNullable(scope)
            .map(s -> s.get(key))
            .filter(Predicate.not(String::isBlank))
            .or(() -> Optional.of(env)
                .map(System::getenv)
                .filter(Predicate.not(String::isBlank)))
            .orElse(fallback);
    }
}
