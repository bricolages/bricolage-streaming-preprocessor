package org.bricolages.streaming;

public class ApplicationAbort extends ApplicationError {
    public ApplicationAbort(String message) {
        super(message);
    }

    public ApplicationAbort(Exception cause) {
        super(cause);
    }
}
