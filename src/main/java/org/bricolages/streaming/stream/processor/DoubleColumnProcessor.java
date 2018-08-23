package org.bricolages.streaming.stream.processor;
import lombok.*;

public class DoubleColumnProcessor extends SingleColumnProcessor {
    static public DoubleColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new DoubleColumnProcessor(params);
    }

    public DoubleColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        double n = Cleanse.getDouble(value);
        if (! Double.isFinite(n)) return null;
        return Double.valueOf(n);
    }
}
