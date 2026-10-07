package com.github.lechandonga.embeddedmessagebrokerjava.broker;

/**
 * Base class for all broker rejections. Carries a stable {@link BrokerErrorCode}
 * that uniquely identifies the rejection reason.
 */
public class BrokerException extends RuntimeException {

    private final BrokerErrorCode code;

    public BrokerException(BrokerErrorCode code, String message) {
        super(message);
        this.code = code;
    }

    public BrokerErrorCode getCode() {
        return code;
    }

    public static BrokerException illegalParameter(String message) {
        return new BrokerException(BrokerErrorCode.ILLEGAL_PARAMETER, message);
    }

    public static BrokerException capacityExceeded(String message) {
        return new BrokerException(BrokerErrorCode.CAPACITY_EXCEEDED, message);
    }

    public static BrokerException duplicateKey(String message) {
        return new BrokerException(BrokerErrorCode.DUPLICATE_KEY, message);
    }

    public static BrokerException noMessageAvailable(String message) {
        return new BrokerException(BrokerErrorCode.NO_MESSAGE_AVAILABLE, message);
    }

    public static BrokerException unknownDelivery(String message) {
        return new BrokerException(BrokerErrorCode.UNKNOWN_DELIVERY, message);
    }

    public static BrokerException offsetRegression(String message) {
        return new BrokerException(BrokerErrorCode.OFFSET_REGRESSION, message);
    }

    public static BrokerException offsetGap(String message) {
        return new BrokerException(BrokerErrorCode.OFFSET_GAP, message);
    }

    public static BrokerException messageInDeadLetter(String message) {
        return new BrokerException(BrokerErrorCode.MESSAGE_IN_DEAD_LETTER, message);
    }

    public static BrokerException deadLetterNotFound(String message) {
        return new BrokerException(BrokerErrorCode.DEAD_LETTER_NOT_FOUND, message);
    }
}
