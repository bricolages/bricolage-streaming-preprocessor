package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import java.util.Objects;
import lombok.*;

public class BooleanColumnProcessor extends SingleColumnProcessor {
    static public final BooleanColumnProcessor create(StreamColumn column) {
        return new BooleanColumnProcessor(column);
    }

    public BooleanColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        if (value instanceof Boolean) {
            return value;
        }
        else if (value instanceof String) {
            if (Objects.equals(value, "true") || Objects.equals(value, "t")) {
                return true;
            }
            else if (Objects.equals(value, "false") || Objects.equals(value, "f")) {
                return false;
            }
            else {
                throw new FilterException("not a boolean: " + value);
            }
        }
        else {
            throw new FilterException("not a boolean: " + value);
        }
    }
}
