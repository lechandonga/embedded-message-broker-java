package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.time.Duration;

/**
 * Tunable broker configuration. All time values are configurable so that tests
 * never need to wait in real time (the broker clock is also injectable).
 *
 * @param visibilityTimeout how long an unacknowledged delivery stays owned by a
 *                          consumer before it becomes redeliverable
 * @param maxAttempts       maximum delivery attempts (1 = no retry); after the
 *                          nack of attempt {@code maxAttempts} the message is dead
 * @param backoffBase       base for exponential backoff between retries
 * @param backoffMultiplier multiplier applied per failed attempt (>= 1)
 * @param backoffMax        cap on a single backoff
 * @param partitionCapacity maximum stored messages per partition (pending +
 *                          inflight + retry-wait); produces beyond this are rejected
 * @param autoCreateTopics  when true, a produce to an unknown topic creates it
 * @param defaultPartitions partition count used for auto-created topics
 */
public record BrokerConfig(Duration visibilityTimeout,
                           int maxAttempts,
                           Duration backoffBase,
                           double backoffMultiplier,
                           Duration backoffMax,
                           int partitionCapacity,
                           boolean autoCreateTopics,
                           int defaultPartitions) {

    public BrokerConfig {
        if (visibilityTimeout == null || visibilityTimeout.isZero() || visibilityTimeout.isNegative()) {
            throw new IllegalArgumentException("visibilityTimeout must be a positive duration");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (backoffBase == null || backoffBase.isNegative()) {
            throw new IllegalArgumentException("backoffBase must not be negative");
        }
        if (backoffMultiplier < 1.0d) {
            throw new IllegalArgumentException("backoffMultiplier must be >= 1");
        }
        if (backoffMax == null || backoffMax.compareTo(backoffBase) < 0) {
            throw new IllegalArgumentException("backoffMax must be >= backoffBase");
        }
        if (partitionCapacity < 1) {
            throw new IllegalArgumentException("partitionCapacity must be >= 1");
        }
        if (defaultPartitions < 1) {
            throw new IllegalArgumentException("defaultPartitions must be >= 1");
        }
    }

    /** Backoff delay that applies after a delivery failed with the given attempt number. */
    public long backoffMillisAfterAttempt(int failedAttempt) {
        long cappedBase = Math.min(backoffBase.toMillis(), backoffMax.toMillis());
        double scaled = cappedBase * Math.pow(backoffMultiplier, Math.max(0, failedAttempt - 1));
        return Math.min(backoffMax.toMillis(), (long) scaled);
    }

    public static BrokerConfig defaults() {
        return new BrokerConfig(Duration.ofSeconds(30), 3,
                Duration.ofMillis(200), 2.0d, Duration.ofSeconds(10), 10_000, true, 1);
    }
}
