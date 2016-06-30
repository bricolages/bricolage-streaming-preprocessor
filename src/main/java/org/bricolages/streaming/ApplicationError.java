package org.bricolages.streaming;

public class ApplicationError extends RuntimeException {
    ApplicationError(String message) {
        super(message);
    }

    ApplicationError(Exception cause) {
        super(cause);
    }
}
