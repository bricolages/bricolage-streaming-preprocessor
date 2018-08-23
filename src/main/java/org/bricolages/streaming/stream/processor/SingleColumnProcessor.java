package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.object.Record;
import lombok.*;

public abstract class SingleColumnProcessor extends StreamColumnProcessor {
    protected SingleColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object process(Record record) {
        val name = params.getSourceName();
        val value = record.get(name);
        record.consume(name);
        try {
            return processValue(value);
        }
        catch (ProcessorException ex) {
            return null;
        }
    }

    public String getSourceName() {
        return params.getSourceName();
    }

    protected abstract Object processValue(Object value) throws ProcessorException;
}
