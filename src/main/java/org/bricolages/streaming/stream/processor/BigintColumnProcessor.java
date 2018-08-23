package org.bricolages.streaming.stream.processor;
import lombok.*;

public class BigintColumnProcessor extends SingleColumnProcessor {
    static public BigintColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new BigintColumnProcessor(params);
    }

    public BigintColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        return Long.valueOf(i);
    }
}
