package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.object.Record;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import java.util.Objects;
import java.util.Map;
import java.util.HashMap;
import java.util.function.BiFunction;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class StreamColumnProcessor {
    final ProcessorParams params;

    public String getDestName() {
        return params.getName();
    }

    public String getSourceName() {
        return params.getSourceName();
    }

    abstract public Object process(Record record);
}
