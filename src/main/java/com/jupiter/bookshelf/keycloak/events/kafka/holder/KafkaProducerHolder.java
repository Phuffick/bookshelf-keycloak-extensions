package com.jupiter.bookshelf.keycloak.events.kafka.holder;

import com.jupiter.bookshelf.keycloak.events.kafka.config.ListenerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class KafkaProducerHolder {

    private static final Logger LOG = System.getLogger(KafkaProducerHolder.class.getName());

    private final Producer<String, String> producer;
    private final String topic;

    public KafkaProducerHolder(ListenerConfig config) {
        this(buildProducer(config), config.topic());
    }

    public KafkaProducerHolder(Producer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    public void publish(String key, String value, String eventId) {
        var headers = List.<Header>of(
            new RecordHeader("id", eventId.getBytes(StandardCharsets.UTF_8)),
            new RecordHeader("contentType", "application/json".getBytes(StandardCharsets.UTF_8))
        );
        var record = new ProducerRecord<>(topic, null, key, value, headers);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                LOG.log(Level.ERROR, "bookshelf-kafka: failed to publish user event (key=" + key + ")", exception);
            }
        });
    }

    public void close() {
        try {
            producer.close(Duration.ofSeconds(5));
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "bookshelf-kafka: error closing Kafka producer", e);
        }
    }

    private static Producer<String, String> buildProducer(ListenerConfig config) {
        var props = buildProperties(config);
        var previous = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(KafkaProducerHolder.class.getClassLoader());
            return new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private static Map<String, Object> buildProperties(ListenerConfig config) {
        var props = new HashMap<String, Object>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, config.clientId());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2_000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 10_000);
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        return props;
    }
}
