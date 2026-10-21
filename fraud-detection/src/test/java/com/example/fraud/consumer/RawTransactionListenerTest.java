package com.example.fraud.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import com.example.fraud.deadletter.DeadLetterPublisher;
import com.example.fraud.validation.InvalidTransactionException;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

@ExtendWith(MockitoExtension.class)
class RawTransactionListenerTest {
    @Mock TransactionProcessor processor;
    @Mock DeadLetterPublisher deadLetters;
    @Mock Acknowledgment acknowledgment;

    @Test void retriesTransientFailureThreeTimesThenRoutesToDeadLetter() throws Exception {
        var registry = new SimpleMeterRegistry();
        var listener = new RawTransactionListener(processor, deadLetters, registry);
        doThrow(new IllegalStateException("temporary failure")).when(processor).process("payload");
        listener.consume("payload", acknowledgment);
        verify(processor, times(3)).process(eq("payload"));
        verify(deadLetters).publish(eq("payload"), eq("PROCESSING_EXHAUSTED"), any(Exception.class));
        verify(acknowledgment).acknowledge();
        org.junit.jupiter.api.Assertions.assertEquals(2, registry.get("fraud.processing.retries").counter().count());
    }

    @Test void invalidMessageSkipsRetriesAndIsDeadLettered() throws Exception {
        var listener = new RawTransactionListener(processor, deadLetters, new SimpleMeterRegistry());
        doThrow(new InvalidTransactionException("invalid")).when(processor).process("bad");
        listener.consume("bad", acknowledgment);
        verify(processor, times(1)).process(eq("bad"));
        verify(deadLetters).publish(eq("bad"), eq("VALIDATION"), any(Exception.class));
        verify(acknowledgment).acknowledge();
    }

    @Test void acknowledgesAfterSuccessfulProcessing() throws Exception {
        var listener = new RawTransactionListener(processor, deadLetters, new SimpleMeterRegistry());
        listener.consume("good", acknowledgment);
        verify(processor).process(eq("good"));
        verifyNoInteractions(deadLetters);
        verify(acknowledgment).acknowledge();
    }
}
