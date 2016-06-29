package org.bricolages.streaming;

public class ApplicationException extends Exception {
    ApplicationException(String message) {
        super(message);
    }

    ApplicationException(Exception cause) {
        super(cause);
    }
}
