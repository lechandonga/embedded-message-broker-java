package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.util.Optional;

/**
 * A message stored inside a partition.
 *
 * @param offset    monotonically increasing offset within the partition
 * @param topic     destination topic
 * @param partition partition index
 * @param key       partition key (also used to order messages)
 * @param dedupKey  optional producer-side idempotency key
 * @param payload   message body
 * @param produceAt broker time (millis) when the message was first accepted
 */
public record StoredMessage(long offset,
                            String topic,
                            int partition,
                            String key,
                            String dedupKey,
                            byte[] payload,
                            long produceAt) {

    public Optional<String> dedupKeyOpt() {
        return Optional.ofNullable(dedupKey);
    }
}
