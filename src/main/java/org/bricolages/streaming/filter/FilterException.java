package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationException;

public class FilterException extends ApplicationException {
    FilterException(String message) {
        super(message);
    }

    FilterException(Exception cause) {
        super(cause);
    }
}
