package org.bricolages.streaming.object;
import org.bricolages.streaming.exception.ApplicationException;

public class ObjectIOException extends ApplicationException {
    public ObjectIOException(String message) {
        super(message);
    }

    public ObjectIOException(Exception cause) {
        super(cause);
    }
}
