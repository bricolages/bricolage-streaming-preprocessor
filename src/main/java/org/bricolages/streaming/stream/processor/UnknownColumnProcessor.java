package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.exception.*;
import lombok.*;

public class UnknownColumnProcessor extends SingleColumnProcessor {
    static public UnknownColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        // just ignore all options
        return new UnknownColumnProcessor(params);
    }

    public UnknownColumnProcessor(ProcessorParams params) {
        super(params);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        return value;
    }
}
