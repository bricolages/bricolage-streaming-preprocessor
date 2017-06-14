package org.bricolages.streaming.preflight;

class StreamDefinitionLoadingException extends RuntimeException {
    StreamDefinitionLoadingException(int columnIndex, String message) {
        super(String.format("column[%d]: %s", columnIndex, message));
    }

    StreamDefinitionLoadingException(int columnIndex, String domainName, String message) {
        super(String.format("column[%d] domain[\"%s\"]: %s", columnIndex, domainName, message));
    }
}
