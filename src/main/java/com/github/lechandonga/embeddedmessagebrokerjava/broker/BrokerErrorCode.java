package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/**
 * Broker operations fail with a distinct, machine-readable reason code so that
 * callers (and tests) can tell why a request was rejected.
 */
public enum BrokerErrorCode {
    ILLEGAL_PARAMETER,
    TOPIC_NOT_FOUND,
    PARTITION_NOT_FOUND,
    CAPACITY_EXCEEDED,
    DUPLICATE_KEY,
    NO_MESSAGE_AVAILABLE,
    UNKNOWN_DELIVERY,
    OFFSET_REGRESSION,
    OFFSET_GAP,
    MESSAGE_IN_DEAD_LETTER,
    DEAD_LETTER_NOT_FOUND,
    UNCONFIRMED_MESSAGE
}
