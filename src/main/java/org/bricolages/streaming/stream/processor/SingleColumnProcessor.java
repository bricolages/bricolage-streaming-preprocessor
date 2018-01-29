package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.Record;
import org.bricolages.streaming.filter.FilterException;
import lombok.*;

public abstract class SingleColumnProcessor extends StreamColumnProcessor {
    protected SingleColumnProcessor(StreamColumn column) {
        super(column);
    }

    @Override
    public Record process(Record record) {
        Object result;
        try {
            result = processValue(record.get(column.getSourceName()), record);
        }
        catch (FilterException ex) {
            result = null;
        }
        if (result == null) {
            record.remove(column.getSourceName());
            return record.isEmpty() ? null : record;
        }
        else {
            record.put(column.getName(), result);
            return record;
        }
    }

    protected abstract Object processValue(Object value, Record record) throws FilterException;
}
