package com.github.lechandonga/embeddedmessagebrokerjava.broker;

/**
 * Result of an offset commit.
 *
 * @param committedOffset new committed offset for the group/partition
 * @param advanced        true when the offset moved forward
 */
public record CommitResult(long committedOffset, boolean advanced) {
}
