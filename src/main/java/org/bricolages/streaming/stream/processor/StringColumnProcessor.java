package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.exception.*;
import lombok.*;

public class StringColumnProcessor extends SingleColumnProcessor {
    static StringColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        return new StringColumnProcessor(column, column.getLength());
    }

    final int length;

    public StringColumnProcessor(StreamColumn column, int length) {
        super(column);
        if (length <= 0) {
            throw new ConfigError("string column requires positive length parameter: " + column.getName());
        }
        this.length = length;
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        if (value instanceof String) {
            return processString((String)value);
        }
        else {
            // varchar column can accept any type of values, just pass through non-string values.
            return value;
        }
    }

    String processString(String value) {
        return removeAfterNullChar(value);
    }

    String removeAfterNullChar(String str) {
        if (str == null) return null;
        int idx = str.indexOf('\0');
        if (idx == -1) return str;
        return str.substring(0, idx);
    }
}
