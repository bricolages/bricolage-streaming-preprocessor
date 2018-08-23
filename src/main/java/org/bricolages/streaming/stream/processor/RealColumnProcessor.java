package org.bricolages.streaming.stream.processor;
import lombok.*;

public class RealColumnProcessor extends SingleColumnProcessor {
    static public RealColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        return new RealColumnProcessor(params);
    }

    public RealColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        float n = Cleanse.getFloat(value);
        if (! Float.isFinite(n)) return null;
        return Float.valueOf(n);
    }
}
