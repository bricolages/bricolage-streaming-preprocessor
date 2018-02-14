package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class SmallintColumnProcessor extends SingleColumnProcessor {
    static SmallintColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        return new SmallintColumnProcessor(column);
    }

    public SmallintColumnProcessor(StreamColumn column) {
        super(column);
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
