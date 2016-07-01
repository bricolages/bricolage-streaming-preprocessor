package org.bricolages.streaming;

public class ConfigError extends ApplicationError {
    public ConfigError(String message) {
        super(message);
    }

    public ConfigError(Exception cause) {
        super(cause);
    }
}
