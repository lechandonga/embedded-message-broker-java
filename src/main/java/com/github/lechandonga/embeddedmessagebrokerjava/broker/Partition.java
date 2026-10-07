package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Single-partition FIFO log with in-flight ownership, per-group offsets,
 * producer deduplication and a dead-letter tail.
 *
 * <p>All mutating operations are synchronized: the partition instance itself is
 * the lock, which also enforces "at most one consumer owns a message in this
 * partition at any moment".</p>
 */
final class Partition {

    private static final class Entry {
        final StoredMessage message;
        MessageStatus status = MessageStatus.PENDING;
        int attempt = 1;
        long deliveryId = -1L;
        long notBeforeMillis = 0L;
        String lastNackReason;

        Entry(StoredMessage message) {
            this.message = message;
        }
    }

    private static final class DedupRecord {
        StoredMessage message;
        String deadLetterId;

        DedupRecord(StoredMessage message) {
            this.message = message;
        }
    }

    private final String topic;
    private final int index;
    private final BrokerConfig config;

    /** Strict offset-ordered live pipeline (pending / inflight / retry-wait / acked). */
    private final Deque<Entry> pipeline = new ArrayDeque<>();
    private final Map<Long, Entry> byOffset = new HashMap<>();
    private final Map<Long, Entry> inflightByDelivery = new HashMap<>();
    private final Map<String, DedupRecord> dedupIndex = new HashMap<>();
    private final Map<String, Long> committedOffsets = new HashMap<>();
    private final List<DeadLetterEntry> deadLetters = new ArrayList<>();

    private long nextOffset;

    Partition(String topic, int index, BrokerConfig config) {
        this.topic = topic;
        this.index = index;
        this.config = config;
    }

    String topic() {
        return topic;
    }

    int index() {
        return index;
    }

    synchronized int liveMessageCount() {
        return pipeline.size();
    }

    synchronized long committedOffset(String group) {
        return committedOffsets.getOrDefault(group, 0L);
    }

    /**
     * Append a message. Rejects with {@link BrokerErrorCode#CAPACITY_EXCEEDED}
     * when the partition is full and {@link BrokerErrorCode#DUPLICATE_KEY} when
     * the dedup key was already accepted (including after replay).
     */
    synchronized ProduceResult produce(String key, String dedupKey, byte[] payload,
                                       long nowMillis, long globalSeq) {
        if (payload == null) {
            throw BrokerException.illegalParameter("payload must not be null");
        }
        if (dedupKey != null && dedupIndex.containsKey(dedupKey)) {
            return ProduceResult.duplicate(dedupIndex.get(dedupKey).message);
        }
        if (pipeline.size() >= config.partitionCapacity()) {
            throw BrokerException.capacityExceeded(
                    "partition " + topic + "-" + index + " is at capacity " + config.partitionCapacity());
        }
        long offset = nextOffset++;
        StoredMessage message = new StoredMessage(offset, topic, index, key, dedupKey, payload, nowMillis);
        Entry entry = new Entry(message);
        pipeline.addLast(entry);
        byOffset.put(offset, entry);
        if (dedupKey != null) {
            dedupIndex.put(dedupKey, new DedupRecord(message));
        }
        return ProduceResult.accepted(message);
    }

    /**
     * Pull the next message for a consumer group. Delivery is strictly
     * offset-ordered: an unacked head message blocks every later message.
     *
     * @return empty optional when nothing is currently deliverable
     */
    synchronized java.util.Optional<Delivery> pull(String group, long nowMillis,
                                                   java.util.function.LongSupplier idGenerator) {
        expireTimedOut(nowMillis);
        for (Entry entry : pipeline) {
            switch (entry.status) {
                case ACKED -> {
                    // Confirmed: no consumer owns it any more, so it does not block
                    // later messages; it simply waits for the offset to be committed.
                    continue;
                }
                case PENDING -> {
                    return java.util.Optional.of(handOut(entry, group, nowMillis, idGenerator));
                }
                case RETRY_WAIT -> {
                    if (entry.notBeforeMillis <= nowMillis) {
                        return java.util.Optional.of(handOut(entry, group, nowMillis, idGenerator));
                    }
                    return java.util.Optional.empty();
                }
                case INFLIGHT -> {
                    return java.util.Optional.empty();
                }
                default -> {
                    return java.util.Optional.empty();
                }
            }
        }
        return java.util.Optional.empty();
    }

    private Delivery handOut(Entry entry, String group, long nowMillis,
                             java.util.function.LongSupplier idGenerator) {
        long deliveryId = idGenerator.getAsLong();
        entry.status = MessageStatus.INFLIGHT;
        entry.deliveryId = deliveryId;
        entry.notBeforeMillis = nowMillis + config.visibilityTimeout().toMillis();
        inflightByDelivery.put(deliveryId, entry);
        return new Delivery(deliveryId, entry.message, group, entry.attempt, entry.notBeforeMillis);
    }

    /**
     * Reclaim inflight messages whose visibility timeout elapsed. The attempt
     * counter is not incremented: a timeout is not a failure, so other messages
     * keep their FIFO position and the redelivered message keeps its retry budget.
     *
     * @return number of deliveries reclaimed
     */
    synchronized int expireTimedOut(long nowMillis) {
        int reclaimed = 0;
        for (Entry entry : pipeline) {
            if (entry.status == MessageStatus.INFLIGHT && entry.notBeforeMillis <= nowMillis) {
                inflightByDelivery.remove(entry.deliveryId);
                entry.status = MessageStatus.PENDING;
                entry.deliveryId = -1L;
                entry.notBeforeMillis = 0L;
                reclaimed++;
            }
        }
        return reclaimed;
    }

    /** Acknowledge a delivery. Only the current owner may ack. */
    synchronized void ack(long deliveryId) {
        Entry entry = requireInflight(deliveryId);
        entry.status = MessageStatus.ACKED;
        entry.deliveryId = -1L;
        inflightByDelivery.remove(deliveryId);
    }

    /**
     * Negatively acknowledge a delivery. Exponential backoff is applied between
     * attempts; once the configured attempt budget is exhausted the message is
     * moved to the dead-letter queue and removed from the live pipeline.
     *
     * @return null when the message stays in the pipeline, otherwise the dead-letter entry
     */
    synchronized DeadLetterEntry nack(long deliveryId, String reason, long nowMillis,
                                      java.util.function.LongSupplier idGenerator) {
        Entry entry = requireInflight(deliveryId);
        inflightByDelivery.remove(deliveryId);
        entry.deliveryId = -1L;
        entry.lastNackReason = reason;
        if (entry.attempt >= config.maxAttempts()) {
            return moveToDeadLetter(entry, reason, nowMillis, idGenerator);
        }
        entry.status = MessageStatus.RETRY_WAIT;
        entry.notBeforeMillis = nowMillis + config.backoffMillisAfterAttempt(entry.attempt);
        entry.attempt++;
        return null;
    }

    private DeadLetterEntry moveToDeadLetter(Entry entry, String reason, long nowMillis,
                                             java.util.function.LongSupplier idGenerator) {
        String deadLetterId = topic + ":" + index + ":" + entry.message.offset()
                + "#" + idGenerator.getAsLong();
        DeadLetterEntry dead = new DeadLetterEntry(deadLetterId, entry.message, null,
                entry.attempt, reason, nowMillis);
        deadLetters.add(dead);
        pipeline.remove(entry);
        byOffset.remove(entry.message.offset());
        entry.status = MessageStatus.DEAD;
        entry.message.dedupKeyOpt().ifPresent(key -> {
            DedupRecord record = dedupIndex.get(key);
            if (record != null) {
                record.deadLetterId = deadLetterId;
                record.message = entry.message;
            }
        });
        return dead;
    }

    private Entry requireInflight(long deliveryId) {
        Entry entry = inflightByDelivery.get(deliveryId);
        if (entry == null) {
            throw BrokerException.unknownDelivery(
                    "delivery " + deliveryId + " is not active in partition "
                            + topic + "-" + index + " (already acked/nacked/timed out, or unknown id)");
        }
        return entry;
    }

    /**
     * Commit the consumed high-watermark for a group (exclusive: offsets
     * {@code < offset} are considered consumed).
     *
     * <ul>
     *   <li>{@code OFFSET_REGRESSION} when the new offset is behind the committed one;</li>
     *   <li>{@code OFFSET_GAP} when it skips an unconfirmed message or exceeds the log end.</li>
     * </ul>
     */
    synchronized CommitResult commit(String group, long offset) {
        long current = committedOffsets.getOrDefault(group, 0L);
        if (offset < current) {
            throw BrokerException.offsetRegression(
                    "cannot commit offset " + offset + " for group '" + group
                            + "': committed offset is already " + current);
        }
        if (offset == current) {
            return new CommitResult(current, false);
        }
        if (offset > nextOffset) {
            throw BrokerException.offsetGap(
                    "cannot commit offset " + offset + ": log ends at " + nextOffset);
        }
        for (Entry entry : pipeline) {
            if (entry.message.offset() >= offset) {
                break;
            }
            if (entry.status != MessageStatus.ACKED) {
                throw new BrokerException(BrokerErrorCode.UNCONFIRMED_MESSAGE,
                        "cannot commit offset " + offset + " for group '" + group
                                + "': message at offset " + entry.message.offset()
                                + " is not acknowledged (status=" + entry.status + ")");
            }
        }
        while (!pipeline.isEmpty() && pipeline.peekFirst().message.offset() < offset) {
            Entry head = pipeline.pollFirst();
            byOffset.remove(head.message.offset());
        }
        committedOffsets.put(group, offset);
        return new CommitResult(offset, true);
    }

    synchronized List<DeadLetterEntry> deadLetterSnapshot() {
        return List.copyOf(deadLetters);
    }

    synchronized DeadLetterEntry findDeadLetter(String deadLetterId) {
        for (DeadLetterEntry dead : deadLetters) {
            if (dead.deadLetterId().equals(deadLetterId)) {
                return dead;
            }
        }
        return null;
    }

    /**
     * Re-inject a dead-letter message at the tail of the same partition with a
     * fresh offset and a reset attempt budget. The dedup index is repointed at
     * the new message, so dedup rules still apply to later produces.
     */
    synchronized StoredMessage replayDeadLetter(String deadLetterId, long nowMillis) {
        DeadLetterEntry dead = findDeadLetter(deadLetterId);
        if (dead == null) {
            throw BrokerException.deadLetterNotFound(
                    "no dead-letter message with id " + deadLetterId
                            + " in partition " + topic + "-" + index);
        }
        if (pipeline.size() >= config.partitionCapacity()) {
            throw BrokerException.capacityExceeded(
                    "partition " + topic + "-" + index + " is at capacity " + config.partitionCapacity());
        }
        long offset = nextOffset++;
        StoredMessage replayed = new StoredMessage(offset, topic, index,
                dead.message().key(), dead.message().dedupKey(), dead.message().payload(), nowMillis);
        pipeline.addLast(new Entry(replayed));
        byOffset.put(offset, pipeline.peekLast());
        deadLetters.remove(dead);
        dead.message().dedupKeyOpt().ifPresent(key -> {
            DedupRecord record = dedupIndex.get(key);
            if (record != null) {
                record.deadLetterId = null;
                record.message = replayed;
            }
        });
        return replayed;
    }
}
