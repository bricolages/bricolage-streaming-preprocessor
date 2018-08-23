package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.exception.ApplicationException;

class OpException extends ApplicationException {
    public OpException(String message) {
        super(message);
    }

    public OpException(Exception cause) {
        super(cause);
    }
}
