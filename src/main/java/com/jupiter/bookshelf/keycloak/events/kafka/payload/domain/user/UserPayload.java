package com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.user;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.ALWAYS)
public record UserPayload(
    String userId,
    String username,
    String email,
    String firstName,
    String lastName
) {
}
