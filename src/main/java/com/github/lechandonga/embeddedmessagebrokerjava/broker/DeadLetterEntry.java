package com.github.lechandonga/embeddedmessagebrokerjava.broker;

/**
 * Snapshot of a message that exhausted its retries.
 */
public record DeadLetterEntry(String deadLetterId,
                              StoredMessage message,
                              String consumerGroup,
                              int attempts,
                              String lastReason,
                              long deadAtMillis) {
}
