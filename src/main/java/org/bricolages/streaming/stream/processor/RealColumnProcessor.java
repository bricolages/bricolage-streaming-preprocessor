package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class RealColumnProcessor extends SingleColumnProcessor {
    static public final RealColumnProcessor create(StreamColumn column) {
        return new RealColumnProcessor(column);
    }

    public RealColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        float n = Cleanse.getFloat(value);
        if (! Float.isFinite(n)) return null;
        return Float.valueOf(n);
    }
}
