package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import java.time.ZoneOffset;
import java.time.OffsetDateTime;
import java.time.DateTimeException;
import lombok.*;

public class TimestampColumnProcessor extends SingleColumnProcessor {
    static public TimestampColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        try {
            if (params.getSourceOffset() == null) {
                throw new ConfigError("source_offset is null: " + params.getName());
            }
            val src = ZoneOffset.of(params.getSourceOffset());
            val dest = (params.getZoneOffset() != null) ? ZoneOffset.of(params.getZoneOffset()) : null;
            return new TimestampColumnProcessor(params, src, dest);
        }
        catch (DateTimeException ex) {
            throw new ConfigError("bad zone offset: " + params.getName() + ": " + ex.getMessage());
        }
    }

    final ZoneOffset sourceOffset;
    final ZoneOffset zoneOffset;

    public TimestampColumnProcessor(ProcessorParams params, ZoneOffset sourceOffset, ZoneOffset zoneOffset) {
        super(params);
        this.sourceOffset = sourceOffset;
        this.zoneOffset = zoneOffset;
    }

    // For tests
    public TimestampColumnProcessor(ProcessorParams params, String sourceOffset, String zoneOffset) {
        this(params, ZoneOffset.of(sourceOffset), (zoneOffset == null) ? null : ZoneOffset.of(zoneOffset));
    }

    @Override
    public Object processValue(Object value) throws FilterException {
        if (value == null) return null;
        val tm = getOffsetDateTime(value);
        if (tm == null) return null;
        return Cleanse.formatSqlTimestamp(tm);
    }

    OffsetDateTime getOffsetDateTime(Object value) throws FilterException {
        if (zoneOffset == null) {
            // Use source offset as-is
            return Cleanse.getOffsetDateTime(value, sourceOffset, true);
        }
        else {
            return Cleanse.getLocalOffsetDateTime(value, sourceOffset, zoneOffset);
        }
    }
}
