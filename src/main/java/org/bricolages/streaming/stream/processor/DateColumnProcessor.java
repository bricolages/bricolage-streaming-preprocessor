package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class DateColumnProcessor extends SingleColumnProcessor {
    static public final DateColumnProcessor create(StreamColumn column) {
        return new DateColumnProcessor(column);
    }

    public DateColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        return Cleanse.formatSqlDate(Cleanse.getLocalDate(value));
    }
}
