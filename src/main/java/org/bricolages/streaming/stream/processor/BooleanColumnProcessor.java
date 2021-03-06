package org.bricolages.streaming.stream.processor;
import java.util.Objects;
import lombok.*;

public class BooleanColumnProcessor extends SingleColumnProcessor {
    static public BooleanColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new BooleanColumnProcessor(params);
    }

    public BooleanColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
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
                throw new ProcessorException("not a boolean: " + value);
            }
        }
        else {
            throw new ProcessorException("not a boolean: " + value);
        }
    }
}
