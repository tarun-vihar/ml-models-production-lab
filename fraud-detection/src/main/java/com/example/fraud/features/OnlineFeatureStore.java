package com.example.fraud.features;

import com.example.fraud.validation.RawTransaction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

@Component
public class OnlineFeatureStore {
    private static final DefaultRedisScript<List> READ_AND_RECORD = new DefaultRedisScript<>(
            "local c=redis.call('HGET',KEYS[1],'count') or '0'; " +
            "local s=redis.call('HGET',KEYS[1],'amount') or '0'; " +
            "local added=redis.call('SADD',KEYS[2],ARGV[1]); " +
            "if added==1 then redis.call('HINCRBY',KEYS[1],'count',1); redis.call('HINCRBYFLOAT',KEYS[1],'amount',ARGV[2]); " +
            "redis.call('EXPIRE',KEYS[1],2592000); redis.call('EXPIRE',KEYS[2],2592000); end; return {c,s};", List.class);
    private final StringRedisTemplate redis;
    public OnlineFeatureStore(StringRedisTemplate redis) { this.redis = redis; }

    /** One Redis script returns pre-event values and then applies an event-ID-deduplicated update. */
    public FeatureSnapshot snapshotThenRecord(RawTransaction event) {
        List<?> values = redis.execute(READ_AND_RECORD,
                List.of("features:account:" + event.accountId() + ":" + event.currency(), "features:seen:" + event.accountId() + ":" + event.currency()),
                event.transactionId(), event.amount().toPlainString());
        if (values == null || values.size() != 2) throw new IllegalStateException("Redis feature script returned no snapshot");
        return new FeatureSnapshot(Long.parseLong(values.get(0).toString()), new java.math.BigDecimal(values.get(1).toString()));
    }
}
