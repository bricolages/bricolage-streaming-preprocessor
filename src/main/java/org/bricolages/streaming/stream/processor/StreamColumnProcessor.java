package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.Record;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import java.util.Objects;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public abstract class StreamColumnProcessor {
    final StreamColumn column;

    static public StreamColumnProcessor create(StreamColumn column) {
        String typeId = column.getType();
        if (Objects.equals(typeId, "integer")) {
            return IntegerColumnProcessor.create(column);
        }
        else if (Objects.equals(typeId, "bigint")) {
            return BigintColumnProcessor.create(column);
        }
        else if (Objects.equals(typeId, "boolean")) {
            return BooleanColumnProcessor.create(column);
        }
        else if (Objects.equals(typeId, "string")) {
            return StringColumnProcessor.create(column);
        }
        else if (Objects.equals(typeId, "timestamp")) {
            return TimestampColumnProcessor.create(column);
        }
/*
        else if (Objects.equals(typeId, "date")) {
            return DateColumnProcessor.createForUnixTime(column);
        }
        else if (Objects.equals(typeId, "float")) {
            return FloatColumnProcessor.create(column);
        }
        // FIXME: should support double type
        //else if (Objects.equals(typeId, "double")) {
        //    return DoubleColumnProcessor.create(column);
        //}
        else if (Objects.equals(typeId, "object")) {
            return FloatColumnProcessor.create(column);
        }
*/
        else {
            throw new ConfigError("unknown column type: column=" + column.getName() + ", type=" + typeId);
        }
    }

    abstract public Record process(Record record);
}
