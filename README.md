# bookshelf-keycloak-extensions

Server-side Keycloak providers for the **bookshelf** platform, packaged as a single shaded jar that
drops into `/opt/keycloak/providers/`.

## What's in it

| Provider | id | Purpose |
|---|---|---|
| `KafkaEventListenerProvider` | `bookshelf-kafka` | On `REGISTER` (and optionally `LOGIN`) publishes a message to the `user-events` Kafka topic so `library-service` can provision the reader's profile. |

More providers (token mappers, authenticators, …) can be added under
`com.jupiter.bookshelf.keycloak.*`; they all ship in the same jar.

## Build

```bash
./gradlew build          # produces build/libs/bookshelf-keycloak-extensions-<version>-all.jar
./gradlew test
```

`org.keycloak:*` are `compileOnly` (provided by the Keycloak distribution). Only `kafka-clients` is
shaded in.

## Publish

Tagging `vX.Y.Z` runs `.github/workflows/publish.yml`, which publishes the `-all` jar to GitHub
Packages at `maven.pkg.github.com/Phuffick/bookshelf-keycloak-extensions`
(`com.jupiter.bookshelf:bookshelf-keycloak-extensions:X.Y.Z`).

The Keycloak image build in `bookshelf-platform` pulls that jar (BuildKit secret
`gh_packages_token`), copies it into `/opt/keycloak/providers/`, and runs `kc.sh build`.

## `bookshelf-kafka` configuration

Each value: Keycloak SPI config first, then the env var, then the default.

| SPI key (`--spi-events-listener-bookshelf-kafka-…`) | Env var | Default |
|---|---|---|
| `bootstrap-servers` | `BOOKSHELF_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `topic` | `BOOKSHELF_KAFKA_TOPIC` | `user-events` |
| `client-id` | `BOOKSHELF_KAFKA_CLIENT_ID` | `keycloak-bookshelf` |
| `realm` | `BOOKSHELF_KAFKA_REALM` | `bookshelf` |
| `emit-login` | `BOOKSHELF_KAFKA_EMIT_LOGIN` | `true` |

Enable it by adding `bookshelf-kafka` to the realm's `eventsListeners`.

## Message contract (`user-events`)

- **key**: Keycloak user id
- **headers**: `id` = a UUID (used by the consumer for idempotency), `contentType` = `application/json`
- **value**:

```json
{
  "publishedEventType": "user.registered",
  "payload": "{\"userId\":\"…\",\"username\":\"jdoe\",\"email\":\"jdoe@example.com\",\"firstName\":\"Jane\",\"lastName\":\"Doe\"}"
}
```

`publishedEventType` is `user.registered` (REGISTER) or `user.login` (LOGIN). `payload` is the inner user JSON
**as a string** (Debezium-outbox shape); inner keys are camelCase. All fields except `userId` are
nullable.
