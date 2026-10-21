package com.example.fraud.persistence;

import com.example.fraud.producer.EnrichedTransaction;
import com.example.fraud.validation.RawTransaction;
import tools.jackson.databind.ObjectMapper;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

@Repository
public class TransactionRepository {
    private final JdbcTemplate jdbc;
    private final ObjectMapper mapper;
    public TransactionRepository(JdbcTemplate jdbc, ObjectMapper mapper) { this.jdbc = jdbc; this.mapper = mapper; }

    @Transactional
    public void persist(RawTransaction raw, EnrichedTransaction enriched) {
        String rawJson = mapper.writeValueAsString(raw);
        String enrichedJson = mapper.writeValueAsString(enriched);
        int inserted = jdbc.update("""
                    INSERT INTO transactions(event_id, transaction_id, account_id, event_timestamp, amount, currency, event_json, enriched_json, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?::jsonb, ?::jsonb, 'ENRICHED') ON CONFLICT DO NOTHING
                    """, raw.eventId(), raw.transactionId(), raw.accountId(), OffsetDateTime.ofInstant(raw.eventTimestamp(), ZoneOffset.UTC), raw.amount(), raw.currency(), rawJson, enrichedJson);
        if (inserted == 1) jdbc.update("INSERT INTO processing_outbox(event_id, transaction_id, payload) VALUES (?, ?, ?::jsonb) ON CONFLICT DO NOTHING",
                    raw.eventId(), raw.transactionId(), enrichedJson);
        if (inserted == 1) jdbc.update("INSERT INTO transaction_audit(event_id, transaction_id, status, details) VALUES (?, ?, 'ENRICHED', ?::jsonb) ON CONFLICT DO NOTHING",
                raw.eventId(), raw.transactionId(), enrichedJson);
    }

    public java.util.List<OutboxRecord> pending(int limit) {
        return jdbc.query("SELECT event_id, transaction_id, payload::text FROM processing_outbox WHERE published = false ORDER BY created_at LIMIT ?",
                (rs, row) -> new OutboxRecord(rs.getString(1), rs.getString(2), rs.getString(3)), limit);
    }
    public void markPublished(String eventId) { jdbc.update("UPDATE processing_outbox SET published = true WHERE event_id = ?", eventId); }
    public record OutboxRecord(String eventId, String transactionId, String payload) {}
}
