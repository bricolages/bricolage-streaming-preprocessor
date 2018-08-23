package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.exception.*;
import java.time.ZoneOffset;
import java.time.OffsetDateTime;
import java.time.DateTimeException;
import lombok.*;

public class DateColumnProcessor extends SingleColumnProcessor {
    static public DateColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        try {
            val src = (params.getSourceOffset() != null ? ZoneOffset.of(params.getSourceOffset()) : ZoneOffset.UTC);
            val dest = (params.getZoneOffset() != null) ? ZoneOffset.of(params.getZoneOffset()) : null;
            return new DateColumnProcessor(params, src, dest);
        }
        catch (DateTimeException ex) {
            throw new ConfigError("bad zone offset: " + params.getName() + ": " + ex.getMessage());
        }
    }

    final ZoneOffset sourceOffset;
    final ZoneOffset zoneOffset;

    public DateColumnProcessor(ProcessorParams params, ZoneOffset sourceOffset, ZoneOffset zoneOffset) {
        super(params);
        this.sourceOffset = sourceOffset;
        this.zoneOffset = zoneOffset;
    }

    // For tests
    DateColumnProcessor(ProcessorParams params, String src, String dest) {
        this(params, (src != null ? ZoneOffset.of(src) : ZoneOffset.UTC), (dest != null ? ZoneOffset.of(dest) : null));
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        return Cleanse.formatSqlDate(Cleanse.getLocalDate(value, sourceOffset, zoneOffset));
    }
}
