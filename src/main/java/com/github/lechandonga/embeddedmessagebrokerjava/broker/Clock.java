package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/** Injectable clock so time-based behaviour (timeout/backoff) is testable without real waiting. */
@FunctionalInterface
public interface Clock {

    long nowMillis();

    Clock SYSTEM = System::currentTimeMillis;
}
