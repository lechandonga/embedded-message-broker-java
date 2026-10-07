package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/** Lifecycle state of a stored message within a partition. */
public enum MessageStatus {
    /** Stored and waiting to be handed to a consumer. */
    PENDING,
    /** Handed to a consumer; awaiting ack/nack or timeout. */
    INFLIGHT,
    /** Consumer reported failure and the message is waiting for its backoff to elapse. */
    RETRY_WAIT,
    /** Consumer acknowledged the message; it may be committed past. */
    ACKED,
    /** Retry budget exhausted; the message lives in the dead-letter queue. */
    DEAD
}
