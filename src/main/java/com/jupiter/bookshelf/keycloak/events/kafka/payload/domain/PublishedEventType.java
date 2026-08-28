package com.jupiter.bookshelf.keycloak.events.kafka.payload.domain;

public enum PublishedEventType {

    USER_REGISTERED_EVENT("user.registered"),
    USER_LOGGED_IN_EVENT("user.login");

    private final String value;

    PublishedEventType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
