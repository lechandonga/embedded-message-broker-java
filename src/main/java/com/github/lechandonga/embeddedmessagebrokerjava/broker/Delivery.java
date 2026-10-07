package com.github.lechandonga/embeddedmessagebrokerjava.broker;

/**
 * A message handed to a consumer by a pull.
 *
 * @param deliveryId     opaque token required for ack/nack
 * @param message        the delivered message
 * @param consumerGroup  consumer group that owns the delivery
 * @param attempt        1 for the first delivery, incremented after each nack
 * @param deadlineMillis broker time after which the delivery may time out
 */
public record Delivery(long deliveryId,
                       StoredMessage message,
                       String consumerGroup,
                       int attempt,
                       long deadlineMillis) {
}
