package com.jupiter.bookshelf.keycloak.events.kafka.payload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator.UserEventPayloadPreparator;
import org.junit.jupiter.api.Test;
import org.keycloak.models.UserModel;

import static com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType.USER_LOGGED_IN_EVENT;
import static com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.PublishedEventType.USER_REGISTERED_EVENT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class UserEventPayloadPreparatorTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final UserEventPayloadPreparator preparator = new UserEventPayloadPreparator();

    @Test
    void prepare_allFields_payloadIsAStringHoldingTheInnerJson() throws Exception {
        var json = preparator.prepare(USER_REGISTERED_EVENT,
            user("3f1e8c2a-9b7d-4c1e-8a2f-0d6b5e4a1c39", "jdoe", "jdoe@example.com", "Jane", "Doe"));

        JsonNode envelope = MAPPER.readTree(json);
        assertThat(envelope.get("eventType").asText()).isEqualTo("user.registered");
        assertThat(envelope.get("payload").isTextual()).isTrue();

        JsonNode payload = MAPPER.readTree(envelope.get("payload").asText());
        assertThat(payload.get("userId").asText()).isEqualTo("3f1e8c2a-9b7d-4c1e-8a2f-0d6b5e4a1c39");
        assertThat(payload.get("username").asText()).isEqualTo("jdoe");
        assertThat(payload.get("email").asText()).isEqualTo("jdoe@example.com");
        assertThat(payload.get("firstName").asText()).isEqualTo("Jane");
        assertThat(payload.get("lastName").asText()).isEqualTo("Doe");
    }

    @Test
    void prepare_nullOptionalFields_serialiseAsJsonNull() throws Exception {
        var json = preparator.prepare(USER_LOGGED_IN_EVENT, user("u1", null, null, null, null));

        JsonNode payload = MAPPER.readTree(MAPPER.readTree(json).get("payload").asText());
        assertThat(payload.get("userId").asText()).isEqualTo("u1");
        assertThat(payload.get("username").isNull()).isTrue();
        assertThat(payload.get("email").isNull()).isTrue();
        assertThat(payload.get("firstName").isNull()).isTrue();
        assertThat(payload.get("lastName").isNull()).isTrue();
    }

    @Test
    void prepare_valuesWithControlCharsAndQuotes_stayValidJsonAndRoundTrip() throws Exception {
        var json = preparator.prepare(USER_REGISTERED_EVENT,
            user("u1", "a\"b\\c", null, "line\nbreak\ttab", null));

        JsonNode payload = MAPPER.readTree(MAPPER.readTree(json).get("payload").asText());
        assertThat(payload.get("username").asText()).isEqualTo("a\"b\\c");
        assertThat(payload.get("firstName").asText()).isEqualTo("line\nbreak\ttab");
    }

    private static UserModel user(String id, String username, String email, String firstName, String lastName) {
        var user = mock(UserModel.class);
        when(user.getId()).thenReturn(id);
        when(user.getUsername()).thenReturn(username);
        when(user.getEmail()).thenReturn(email);
        when(user.getFirstName()).thenReturn(firstName);
        when(user.getLastName()).thenReturn(lastName);
        return user;
    }
}
