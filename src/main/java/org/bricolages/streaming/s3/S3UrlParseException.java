package org.bricolages.streaming.s3;
import org.bricolages.streaming.ApplicationException;

public class S3UrlParseException extends ApplicationException {
    S3UrlParseException(String message) {
        super(message);
    }

    S3UrlParseException(Exception cause) {
        super(cause);
    }
}
