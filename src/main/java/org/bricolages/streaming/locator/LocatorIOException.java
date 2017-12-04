package org.bricolages.streaming.locator;
import org.bricolages.streaming.exception.ApplicationException;

public class LocatorIOException extends ApplicationException {
    LocatorIOException(String message) {
        super(message);
    }

    LocatorIOException(Exception cause) {
        super(cause);
    }
}
