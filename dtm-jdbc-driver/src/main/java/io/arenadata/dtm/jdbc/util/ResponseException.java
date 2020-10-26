package io.arenadata.dtm.jdbc.util;

/**
 * Искючение полученное от сервера в теле.
 */
public class ResponseException {

    private String exceptionMessage;

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    public void setExceptionMessage(String exceptionMessage) {
        this.exceptionMessage = exceptionMessage;
    }
}
