package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class IntegerColumnProcessor extends SingleColumnProcessor {
    static public final IntegerColumnProcessor create(StreamColumn column) {
        return new IntegerColumnProcessor(column);
    }

    public IntegerColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value, Record record) throws FilterException {
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
