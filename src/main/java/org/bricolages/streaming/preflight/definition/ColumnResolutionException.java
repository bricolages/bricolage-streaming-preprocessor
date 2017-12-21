package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.exception.*;

class ColumnResolutionException extends ApplicationError {
    ColumnResolutionException(String message) {
        super(message);
    }
}
