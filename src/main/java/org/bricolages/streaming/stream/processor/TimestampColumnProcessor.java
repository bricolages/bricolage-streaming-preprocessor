package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import java.time.ZoneOffset;
import java.time.OffsetDateTime;
import lombok.*;

public class TimestampColumnProcessor extends SingleColumnProcessor {
    static TimestampColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        return new TimestampColumnProcessor(column, column.getSourceOffset(), column.getZoneOffset());
    }

    final ZoneOffset sourceOffset;
    final ZoneOffset zoneOffset;

    public TimestampColumnProcessor(StreamColumn column, String sourceOffset, String zoneOffset) {
        super(column);
        this.sourceOffset = ZoneOffset.of(sourceOffset);
        this.zoneOffset = ZoneOffset.of(zoneOffset);
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        OffsetDateTime tm = Cleanse.getLocalOffsetDateTime(value, sourceOffset, zoneOffset);
        if (tm == null) return null;
        return Cleanse.formatSqlTimestamp(tm);
    }
}
