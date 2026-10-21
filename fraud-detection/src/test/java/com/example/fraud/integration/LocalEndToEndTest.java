package com.example.fraud.integration;

import static org.junit.jupiter.api.Assertions.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@Testcontainers
class LocalEndToEndTest {
    @Container static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @DynamicPropertySource static void infrastructure(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }
    @Autowired KafkaTemplate<String, String> kafka;
    @Autowired JdbcTemplate jdbc;
    @Autowired StringRedisTemplate redis;

    @Test void consumesPersistsEnrichesAndRoutesInvalidMessage() throws Exception {
        try (var enriched = consumer("enriched-transactions"); var dead = consumer("dead-letter-transactions")) {
            String firstId = "e2e-" + java.util.UUID.randomUUID();
            String firstPayload = transaction(firstId, firstId + "-tx", "12.50");
            kafka.send("raw-transactions", "e2e-account", firstPayload).get();
            var first = waitFor(enriched, firstId);
            assertEquals(0, first.get("priorTransactionCount").asInt());
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM transactions WHERE event_id = ?", Integer.class, firstId));
            kafka.send("raw-transactions", "e2e-account", firstPayload).get();

            String secondId = firstId + "-second";
            kafka.send("raw-transactions", "e2e-account", transaction(secondId, secondId + "-tx", "8.00")).get();
            var second = waitFor(enriched, secondId);
            assertEquals(1, second.get("priorTransactionCount").asInt(), "the new transaction must see only prior history");
            assertEquals(1, jdbc.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Integer.class, firstId + "-tx"));
            assertEquals("2", redis.opsForHash().get("features:account:e2e-account:USD", "count"));

            kafka.send("raw-transactions", "e2e-account", "{\"eventId\":\"bad\",\"schemaVersion\":999}").get();
            var deadLetter = waitFor(dead, null);
            assertEquals("VALIDATION", deadLetter.get("errorCategory").asText());
            assertTrue(deadLetter.has("originalPayload"));
        }
    }
    private KafkaConsumer<String, String> consumer(String topic) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "e2e-" + topic + "-" + java.util.UUID.randomUUID());
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        var c = new KafkaConsumer<String, String>(p); c.subscribe(java.util.List.of(topic)); return c;
    }
    private tools.jackson.databind.JsonNode waitFor(KafkaConsumer<String, String> consumer, String eventId) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        var mapper = new tools.jackson.databind.ObjectMapper();
        while (System.nanoTime() < deadline) {
            for (var record : consumer.poll(Duration.ofMillis(500))) {
                var json = mapper.readTree(record.value());
                if (eventId == null || eventId.equals(json.path("eventId").asText())) return json;
            }
        }
        fail("Timed out waiting for event " + eventId);
        return null;
    }
    private String transaction(String eventId, String transactionId, String amount) {
        return """
                {"eventId":"%s","transactionId":"%s","schemaVersion":1,"eventType":"TRANSACTION_AUTHORIZED",
                "eventTimestamp":"%s","accountId":"e2e-account","amount":%s,"currency":"USD",
                "merchantId":"merchant-1","deviceId":"device-1","paymentInstrumentId":"card-1"}
                """.formatted(eventId, transactionId, Instant.now(), amount);
    }
}
