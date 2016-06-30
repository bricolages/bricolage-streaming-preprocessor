package org.bricolages.streaming;

public class MessageParseException extends ApplicationException {
    MessageParseException(String message) {
        super(message);
    }

    MessageParseException(Exception cause) {
        super(cause);
    }
}
