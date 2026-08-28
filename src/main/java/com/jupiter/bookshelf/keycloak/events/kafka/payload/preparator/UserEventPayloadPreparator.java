package com.jupiter.bookshelf.keycloak.events.kafka.payload.preparator;

import com.jupiter.bookshelf.keycloak.events.kafka.payload.domain.user.UserPayload;
import org.keycloak.models.UserModel;

public class UserEventPayloadPreparator extends AbstractEventPayloadPreparator<UserModel> {

    public UserEventPayloadPreparator() {
        super();
    }

    @Override
    protected UserPayload toPayload(UserModel user) {
        return new UserPayload(
            user.getId(),
            user.getUsername(),
            user.getEmail(),
            user.getFirstName(),
            user.getLastName()
        );
    }
}
