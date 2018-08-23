package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.exception.*;
import lombok.*;

public class StringColumnProcessor extends SingleColumnProcessor {
    static public StringColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        if (params.getLength() == null) {
            throw new ConfigError("length is required for string type: " + params.getName());
        }
        return new StringColumnProcessor(params, (int)params.getLength());
    }

    final int length;

    public StringColumnProcessor(ProcessorParams params, int length) {
        super(params);
        if (length <= 0) {
            throw new ConfigError("string column requires positive length parameter: " + params.getName());
        }
        this.length = length;
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
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
