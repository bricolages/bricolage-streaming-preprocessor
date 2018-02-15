package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class BigintColumnProcessor extends SingleColumnProcessor {
    static BigintColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        return new BigintColumnProcessor(column);
    }

    public BigintColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        return Long.valueOf(i);
    }
}
