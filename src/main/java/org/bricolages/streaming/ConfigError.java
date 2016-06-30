package org.bricolages.streaming;

public class ConfigError extends ApplicationError {
    ConfigError(String message) {
        super(message);
    }

    ConfigError(Exception cause) {
        super(cause);
    }
}
