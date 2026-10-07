package com.github.lechandonga/embeddedmessagebrokerjava.broker;

/**
 * Result of a produce request.
 *
 * @param accepted     false when an identical dedup key had already been accepted
 * @param offset       partition offset of the (first) accepted message
 * @param partition    partition the message was assigned to
 * @param duplicate    true when this call was deduplicated against an earlier one
 * @param existingOffset offset of the original message when {@code duplicate} is true
 */
public record ProduceResult(boolean accepted,
                            long offset,
                            int partition,
                            boolean duplicate,
                            long existingOffset) {

    static ProduceResult accepted(StoredMessage message) {
        return new ProduceResult(true, message.offset(), message.partition(), false, -1L);
    }

    static ProduceResult duplicate(StoredMessage existing) {
        return new ProduceResult(false, existing.offset(), existing.partition(), true, existing.offset());
    }
}
