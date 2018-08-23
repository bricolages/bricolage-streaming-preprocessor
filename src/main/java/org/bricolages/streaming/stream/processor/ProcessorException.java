package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.exception.ApplicationException;

class ProcessorException extends ApplicationException {
    public ProcessorException(String message) {
        super(message);
    }

    public ProcessorException(Exception cause) {
        super(cause);
    }
}
