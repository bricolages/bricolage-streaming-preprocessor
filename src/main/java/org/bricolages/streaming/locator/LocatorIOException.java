package org.bricolages.streaming.locator;
import org.bricolages.streaming.exception.ApplicationException;

public class LocatorIOException extends ApplicationException {
    public LocatorIOException(String message) {
        super(message);
    }

    public LocatorIOException(Exception cause) {
        super(cause);
    }
}
