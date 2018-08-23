package org.bricolages.streaming.object;
import org.bricolages.streaming.exception.ApplicationException;

public class JSONParseException extends ApplicationException {
    JSONParseException(String message) {
        super(message);
    }

    JSONParseException(Exception cause) {
        super(cause);
    }
}
