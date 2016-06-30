package org.bricolages.streaming;

public class SQSException extends ApplicationError {
    SQSException(String message) {
        super(message);
    }

    SQSException(Exception cause) {
        super(cause);
    }
}
