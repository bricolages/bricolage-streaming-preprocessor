package org.bricolages.streaming.exception;

public class ApplicationException extends Exception {
    public ApplicationException(String message) {
        super(message);
    }

    public ApplicationException(Exception cause) {
        super(cause);
    }
}
