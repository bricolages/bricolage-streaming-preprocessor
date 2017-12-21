package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.exception.*;

class DomainResolutionException extends ApplicationError {
    DomainResolutionException(String message) {
        super(message);
    }
}
