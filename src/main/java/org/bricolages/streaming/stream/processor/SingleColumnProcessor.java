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
    public Object process(Record record) {
        val name = column.getSourceName();
        val value = record.get(name);
        record.consume(name);
        try {
            return processValue(value);
        }
        catch (FilterException ex) {
            return null;
        }
    }

    public String getSourceName() {
        return column.getSourceName();
    }

    protected abstract Object processValue(Object value) throws FilterException;
}
