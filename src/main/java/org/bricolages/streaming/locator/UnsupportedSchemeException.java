package org.bricolages.streaming.locator;
import org.bricolages.streaming.exception.*;

public class UnsupportedSchemeException extends ApplicationError {
    static final long serialVersionUID = 1L;

    UnsupportedSchemeException(String message) {
        super(message);
    }
}
