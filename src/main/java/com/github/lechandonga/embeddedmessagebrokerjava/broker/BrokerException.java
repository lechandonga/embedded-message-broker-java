package com.github.lechandonga.embeddedmessagebrokerjava.broker;

public class BrokerException extends RuntimeException {

    private final ErrorCode code;

    public BrokerException(ErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }
}
