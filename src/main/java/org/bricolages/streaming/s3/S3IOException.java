package org.bricolages.streaming.s3;
import org.bricolages.streaming.ApplicationException;

public class S3IOException extends ApplicationException {
    S3IOException(String message) {
        super(message);
    }

    S3IOException(Exception cause) {
        super(cause);
    }
}
