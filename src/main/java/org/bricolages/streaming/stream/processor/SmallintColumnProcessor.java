package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class SmallintColumnProcessor extends SingleColumnProcessor {
    static public SmallintColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new SmallintColumnProcessor(params);
    }

    public SmallintColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        if (Short.MIN_VALUE <= i && i <= Short.MAX_VALUE) {
            return Short.valueOf((short)i);
        }
        else {
            return null;
        }
    }
}
