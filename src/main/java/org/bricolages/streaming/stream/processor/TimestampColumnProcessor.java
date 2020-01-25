package org.bricolages.streaming.stream.processor;

import org.bricolages.streaming.exception.*;

import java.time.ZoneOffset;
import java.time.OffsetDateTime;
import java.time.DateTimeException;
import java.util.concurrent.TimeUnit;

import lombok.Getter;

public class TimestampColumnProcessor extends SingleColumnProcessor {
    static public TimestampColumnProcessor build(ProcessorParams params, ProcessorContext ctx) {
        try {
            if (params.getSourceOffset() == null) {
                throw new ConfigError("source_offset is null: " + params.getName());
            }
            var src = ZoneOffset.of(params.getSourceOffset());
            var dest = (params.getZoneOffset() != null) ? ZoneOffset.of(params.getZoneOffset()) : null;
            var unit = (params.getTimeUnit() != null) ? TimeUnit.valueOf(params.getTimeUnit().toUpperCase()) : TimeUnit.SECONDS;
            switch (unit) {
            case SECONDS:
            case MILLISECONDS:
                break;
            default:
                throw new ConfigError("unsupported time unit: column " + params.getName() + ": " + unit);
            }
            return new TimestampColumnProcessor(params, src, dest, unit);
        }
        catch (DateTimeException ex) {
            throw new ConfigError("bad zone offset: column " + params.getName() + ": " + ex.getMessage());
        }
        catch (IllegalArgumentException ex) {
            throw new ConfigError("bad time unit: column " + params.getName() + ": " + params.getTimeUnit());
        }
    }

    @Getter final ZoneOffset sourceOffset;
    @Getter final ZoneOffset zoneOffset;
    @Getter final TimeUnit timeUnit;

    public TimestampColumnProcessor(ProcessorParams params, ZoneOffset sourceOffset, ZoneOffset zoneOffset, TimeUnit timeUnit) {
        super(params);
        this.sourceOffset = sourceOffset;
        this.zoneOffset = zoneOffset;
        this.timeUnit = timeUnit;
    }

    // For tests
    public TimestampColumnProcessor(ProcessorParams params, String sourceOffset, String zoneOffset, TimeUnit timeUnit) {
        this(params, ZoneOffset.of(sourceOffset), (zoneOffset == null) ? null : ZoneOffset.of(zoneOffset), timeUnit);
    }

    @Override
    public Object processValue(Object value) throws ProcessorException {
        if (value == null) return null;
        var tm = getOffsetDateTime(value);
        if (tm == null) return null;
        return Cleanse.formatSqlTimestamp(tm);
    }

    OffsetDateTime getOffsetDateTime(Object value) throws ProcessorException {
        if (zoneOffset == null) {
            // Use source offset as-is
            return Cleanse.getOffsetDateTime(value, sourceOffset);
        }
        else {
            return Cleanse.getLocalOffsetDateTime(value, sourceOffset, zoneOffset, this.timeUnit);
        }
    }
}
