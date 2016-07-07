package org.bricolages.streaming.filter;
import org.bricolages.streaming.ApplicationException;

public class JSONException extends ApplicationException {
    JSONException(String message) {
        super(message);
    }

    JSONException(Exception cause) {
        super(cause);
    }
}
