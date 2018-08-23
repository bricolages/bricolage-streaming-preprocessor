package org.bricolages.streaming.object;
import org.bricolages.streaming.exception.ApplicationException;

public class LocatorParseException extends ApplicationException {
    LocatorParseException(String message) {
        super(message);
    }

    LocatorParseException(Exception cause) {
        super(cause);
    }
}
