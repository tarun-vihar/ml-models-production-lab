package com.example.fraud.deadletter;

import com.example.fraud.config.FraudProperties;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;
import java.time.Instant;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class DeadLetterPublisher {
    private final KafkaTemplate<String, String> kafka;
    private final FraudProperties properties;
    private final ObjectMapper mapper;
    public DeadLetterPublisher(KafkaTemplate<String, String> kafka, FraudProperties properties, ObjectMapper mapper) { this.kafka = kafka; this.properties = properties; this.mapper = mapper; }

    public void publish(String original, String category, Exception error) throws Exception {
        ObjectNode payload = mapper.createObjectNode();
        payload.put("originalPayload", original);
        payload.put("errorCategory", category);
        payload.put("error", safeMessage(error));
        payload.put("failedAt", Instant.now().toString());
        try {
            var source = mapper.readTree(original);
            var identifiers = mapper.createObjectNode();
            if (source.has("eventId")) identifiers.set("eventId", source.get("eventId"));
            if (source.has("transactionId")) identifiers.set("transactionId", source.get("transactionId"));
            if (source.has("accountId")) identifiers.set("accountId", source.get("accountId"));
            payload.set("eventIdentifiers", identifiers);
        } catch (Exception ignored) { payload.set("eventIdentifiers", mapper.createObjectNode()); }
        kafka.send(properties.topics().deadLetter(), payload.toString()).get();
    }
    private String safeMessage(Exception e) { return e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage(); }
}
