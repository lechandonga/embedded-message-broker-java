package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.time.Instant;

/**
 * 不可变消息视图，对外暴露。
 */
public record Message(
        String id,
        String topic,
        String partitionKey,
        long offset,
        String payload,
        String dedupKey,
        int attempts,
        Instant visibleAt) {
}
