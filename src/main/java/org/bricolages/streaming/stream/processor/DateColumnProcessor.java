package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import lombok.*;

public class DateColumnProcessor extends SingleColumnProcessor {
    static DateColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        return new DateColumnProcessor(column);
    }

    public DateColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        return Cleanse.formatSqlDate(Cleanse.getLocalDate(value));
    }
}
