package com.github.lechandonga.embeddedmessagebrokerjava.broker;

import java.time.Instant;

public record DeadLetter(
        String deadLetterId,
        Message message,
        String reason,
        Instant deadAt) {
}
