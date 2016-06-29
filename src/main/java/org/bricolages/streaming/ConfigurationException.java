package org.bricolages.streaming;

public class ConfigurationException extends ApplicationException {
    ConfigurationException(String message) {
        super(message);
    }

    ConfigurationException(Exception cause) {
        super(cause);
    }
}
