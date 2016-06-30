package org.bricolages.streaming;

public class ApplicationAbort extends ApplicationError {
    ApplicationAbort(String message) {
        super(message);
    }

    ApplicationAbort(Exception cause) {
        super(cause);
    }
}
