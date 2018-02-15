package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationException;

public class FilterException extends ApplicationException {
    public FilterException(String message) {
        super(message);
    }

    public FilterException(Exception cause) {
        super(cause);
    }
}
