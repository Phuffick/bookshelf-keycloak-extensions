package com.jupiter.bookshelf.keycloak.events.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jupiter.bookshelf.keycloak.events.kafka.config.ListenerConfig;
import com.jupiter.bookshelf.keycloak.events.kafka.holder.KafkaProducerHolder;
import com.jupiter.bookshelf.keycloak.events.kafka.listener.provider.KafkaEventListenerProvider;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator.UserEventPayloadPreparator;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.EventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.UserLoggedInEventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.UserRegisteredEventPublicationStrategy;
import com.jupiter.bookshelf.keycloak.events.kafka.strategy.registry.EventPublicationStrategyRegistryImpl;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.keycloak.events.Event;
import org.keycloak.events.EventType;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.RealmModel;
import org.keycloak.models.RealmProvider;
import org.keycloak.models.UserModel;
import org.keycloak.models.UserProvider;
import org.mockito.ArgumentCaptor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaEventListenerProviderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String REALM_ID = "realm-id";
    private static final String USER_ID = "3f1e8c2a-9b7d-4c1e-8a2f-0d6b5e4a1c39";

    private KeycloakSession session;
    private RealmModel realm;
    private Producer<String, String> producer;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        session = mock(KeycloakSession.class);
        realm = mock(RealmModel.class);
        producer = mock(Producer.class);
        var realmProvider = mock(RealmProvider.class);
        var userProvider = mock(UserProvider.class);
        var user = mock(UserModel.class);

        when(session.realms()).thenReturn(realmProvider);
        when(session.users()).thenReturn(userProvider);
        when(realmProvider.getRealm(REALM_ID)).thenReturn(realm);
        when(realm.getName()).thenReturn("bookshelf");
        when(userProvider.getUserById(eq(realm), eq(USER_ID))).thenReturn(user);
        when(user.getId()).thenReturn(USER_ID);
        when(user.getUsername()).thenReturn("jdoe");
        when(user.getEmail()).thenReturn("jdoe@example.com");
        when(user.getFirstName()).thenReturn("Jane");
        when(user.getLastName()).thenReturn("Doe");
    }

    @Test
    void onEvent_registerEvent_publishesUserRegisteredEnvelopeKeyedByUserId() throws Exception {
        provider(config(true)).onEvent(event(EventType.REGISTER));

        var record = captureSent();
        assertThat(record.topic()).isEqualTo("user-events");
        assertThat(record.key()).isEqualTo(USER_ID);

        JsonNode envelope = MAPPER.readTree(record.value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("user.registered");
        JsonNode payload = MAPPER.readTree(envelope.get("payload").asText());
        assertThat(payload.get("userId").asText()).isEqualTo(USER_ID);
        assertThat(payload.get("firstName").asText()).isEqualTo("Jane");

        var idHeader = new String(record.headers().lastHeader("id").value(), StandardCharsets.UTF_8);
        assertThatCode(() -> UUID.fromString(idHeader)).doesNotThrowAnyException();
        assertThat(record.headers().lastHeader("contentType").value())
            .isEqualTo("application/json".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void onEvent_loginEventAndEmitLoginEnabled_publishesUserLoginEnvelope() throws Exception {
        provider(config(true)).onEvent(event(EventType.LOGIN));

        JsonNode envelope = MAPPER.readTree(captureSent().value());
        assertThat(envelope.get("eventType").asText()).isEqualTo("user.login");
    }

    @Test
    void onEvent_loginEventAndEmitLoginDisabled_publishesNothing() {
        provider(config(false)).onEvent(event(EventType.LOGIN));

        verify(producer, never()).send(any(), any());
    }

    @Test
    void onEvent_unrelatedEventType_publishesNothing() {
        provider(config(true)).onEvent(event(EventType.LOGOUT));

        verify(producer, never()).send(any(), any());
    }

    @Test
    void onEvent_eventFromAnotherRealm_publishesNothing() {
        when(realm.getName()).thenReturn("master");

        provider(config(true)).onEvent(event(EventType.REGISTER));

        verify(producer, never()).send(any(), any());
    }

    @Test
    void onEvent_eventWithNoUserId_publishesNothing() {
        var event = event(EventType.REGISTER);
        when(event.getUserId()).thenReturn(null);

        provider(config(true)).onEvent(event);

        verify(producer, never()).send(any(), any());
    }

    @Test
    void onEvent_producerFailure_isSwallowed() {
        doThrow(new KafkaException("boom")).when(producer).send(any(), any());

        assertThatCode(() -> provider(config(true)).onEvent(event(EventType.REGISTER)))
            .doesNotThrowAnyException();
    }

    private KafkaEventListenerProvider provider(ListenerConfig config) {
        var payloadPreparator = new UserEventPayloadPreparator();
        List<EventPublicationStrategy> strategies = List.of(
            new UserRegisteredEventPublicationStrategy(payloadPreparator),
            new UserLoggedInEventPublicationStrategy(payloadPreparator, config)
        );
        return new KafkaEventListenerProvider(session,
            new KafkaProducerHolder(producer, "user-events"),
            new EventPublicationStrategyRegistryImpl(strategies),
            config);
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, String> captureSent() {
        var captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(producer).send(captor.capture(), any());
        return captor.getValue();
    }

    private static ListenerConfig config(boolean emitLogin) {
        return new ListenerConfig("localhost:9092", "user-events", "kc-test", "bookshelf", emitLogin);
    }

    private static Event event(EventType type) {
        var event = mock(Event.class);
        when(event.getType()).thenReturn(type);
        when(event.getRealmId()).thenReturn(REALM_ID);
        when(event.getUserId()).thenReturn(USER_ID);
        return event;
    }
}
