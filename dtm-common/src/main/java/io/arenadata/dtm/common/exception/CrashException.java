package io.arenadata.dtm.common.exception;

public class CrashException extends RuntimeException {

    private static final String MESSAGE = "Crash exception";

    public CrashException() {
        super(MESSAGE);
    }

    public CrashException(String message, Throwable cause) {
        super(message, cause);
    }
}
