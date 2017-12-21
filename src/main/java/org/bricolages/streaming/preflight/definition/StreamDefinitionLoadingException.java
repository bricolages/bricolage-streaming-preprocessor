package org.bricolages.streaming.preflight.definition;
import org.bricolages.streaming.exception.*;

class StreamDefinitionLoadingException extends ApplicationError {
    StreamDefinitionLoadingException(int columnIndex, String message) {
        super(String.format("column[%d]: %s", columnIndex, message));
    }

    StreamDefinitionLoadingException(int columnIndex, String domainName, String message) {
        super(String.format("column[%d] domain[\"%s\"]: %s", columnIndex, domainName, message));
    }
}
