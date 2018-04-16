package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class IntegerColumnProcessor extends SingleColumnProcessor {
    static public IntegerColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new IntegerColumnProcessor(params);
    }

    public IntegerColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        if (Integer.MIN_VALUE <= i && i <= Integer.MAX_VALUE) {
            return Integer.valueOf((int)i);
        }
        else {
            return null;
        }
    }
}
