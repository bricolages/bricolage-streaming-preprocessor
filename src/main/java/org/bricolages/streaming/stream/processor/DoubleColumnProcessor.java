package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class DoubleColumnProcessor extends SingleColumnProcessor {
    static public final DoubleColumnProcessor create(StreamColumn column) {
        return new DoubleColumnProcessor(column);
    }

    public DoubleColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        double n = Cleanse.getDouble(value);
        if (! Double.isFinite(n)) return null;
        return Double.valueOf(n);
    }
}
