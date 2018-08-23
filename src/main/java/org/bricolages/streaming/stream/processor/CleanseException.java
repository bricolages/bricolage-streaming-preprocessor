package org.bricolages.streaming.stream.processor;

public class CleanseException extends ProcessorException {
    public CleanseException(String message) {
        super(message);
    }

    public CleanseException(Exception cause) {
        super(cause);
    }
}
