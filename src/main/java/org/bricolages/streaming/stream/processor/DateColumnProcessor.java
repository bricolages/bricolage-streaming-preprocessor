package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.exception.*;
import java.time.ZoneOffset;
import java.time.OffsetDateTime;
import java.time.DateTimeException;
import lombok.*;

public class DateColumnProcessor extends SingleColumnProcessor {
    static DateColumnProcessor build(StreamColumn column, ProcessorContext ctx) {
        try {
            val src = (column.getSourceOffset() != null ? ZoneOffset.of(column.getSourceOffset()) : ZoneOffset.UTC);
            val dest = (column.getZoneOffset() != null) ? ZoneOffset.of(column.getZoneOffset()) : null;
            return new DateColumnProcessor(column, src, dest);
        }
        catch (DateTimeException ex) {
            throw new ConfigError("bad zone offset: " + column.getName() + ": " + ex.getMessage());
        }
    }

    final ZoneOffset sourceOffset;
    final ZoneOffset zoneOffset;

    public DateColumnProcessor(StreamColumn column, ZoneOffset sourceOffset, ZoneOffset zoneOffset) {
        super(column);
        this.sourceOffset = sourceOffset;
        this.zoneOffset = zoneOffset;
    }

    // For tests
    DateColumnProcessor(StreamColumn column, String src, String dest) {
        this(column, (src != null ? ZoneOffset.of(src) : ZoneOffset.UTC), (dest != null ? ZoneOffset.of(dest) : null));
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        return Cleanse.formatSqlDate(Cleanse.getLocalDate(value, sourceOffset, zoneOffset));
    }
}
