package org.bricolages.streaming.filter;
import org.bricolages.streaming.ConfigError;
import java.util.function.Function;
import java.util.Map;
import java.util.HashMap;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public abstract class Op {
    static final Map<String, Function<OperatorDefinition, Op>> BUILDERS = new HashMap<String, Function<OperatorDefinition, Op>>();

    static final public void registerOperator(String id, Function<OperatorDefinition, Op> builder) {
        log.debug("new operator builder registered: '{}' -> {}", id, builder);
        BUILDERS.put(id, builder);
    }

    static {
        IntOp.register();
        BigIntOp.register();
        TextOp.register();
        TimeZoneOp.register();
        UnixTimeOp.register();
        RemoveNullOp.register();
        AggregateOp.register();
    }

    static final public Op build(OperatorDefinition def) {
        val builder = BUILDERS.get(def.getOperatorId());
        if (builder == null) {
            throw new ConfigError("unknown operator ID: " + def.getOperatorId());
        }
        return builder.apply(def);
    }

    final OperatorDefinition def;

    Op(OperatorDefinition def) {
        this.def = def;
    }

    protected String getColumnName() {
        return def.getTargetColumn();
    }

    public abstract Record apply(Record record);

    protected long getInteger(Object value) throws FilterException {
        if (value instanceof Integer) {
            return ((Integer)value).longValue();
        }
        else if (value instanceof Long) {
            return ((Long)value).longValue();
        }
        else if (value instanceof String) {
            try {
                return Long.valueOf((String)value);
            }
            catch (NumberFormatException ex) {
                throw new FilterException(ex);
            }
        }
        else if (value instanceof Float) {
            return ((Float)value).longValue();
        }
        else if (value instanceof Double) {
            return ((Double)value).longValue();
        }
        else {
            throw new FilterException("unexpected value for integer");
        }
    }

    protected OffsetDateTime unixTimeToOffsetDateTime(long t, ZoneOffset offset) throws FilterException {
        try {
            return Instant.ofEpochSecond(t).atOffset(offset);
        }
        catch (DateTimeException ex) {
            throw new FilterException(ex);
        }
    }

    // Redshift does not support timezone, but COPY just ignores zone.
    // So keeping timezone information in the JSON data file does not hurt loading task and is a good thing.
    static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    protected String formatSqlTimestamp(OffsetDateTime dt) throws FilterException {
        try {
            return dt.format(SQL_TIMESTAMP_FORMAT);
        }
        catch (DateTimeException ex) {
            throw new FilterException(ex);
        }
    }

    static protected final DateTimeFormatter RUBY_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss xxxx");

    protected OffsetDateTime getOffsetDateTime(Object value, ZoneOffset defaultOffset) throws FilterException {
        if (value instanceof String) {
            val str = ((String)value).trim();
            // Order DOES matter
            try {
                // "2016-07-01T12:34:56Z": This representation appears most
                return OffsetDateTime.parse(str, DateTimeFormatter.ISO_INSTANT);
            }
            catch (DateTimeException e1) {
                try {
                    // "2016-07-01T12:34:56+00:00": Some log file have this.  Old Fluentd format?
                    return OffsetDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
                }
                catch (DateTimeException e2) {
                    try {
                        // "2016-07-01 12:34:56 +0000": Ruby Time#to_s
                        return OffsetDateTime.parse(str, RUBY_DATE_TIME);
                    }
                    catch (DateTimeException e3) {
                        try {
                            // "2016-07-01T12:34:56": No offset.
                            return LocalDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME).atOffset(defaultOffset);
                        }
                        catch (DateTimeException e4) {
                            throw new FilterException("could not parse a timestamp: " + str);
                        }
                    }
                }
            }
        }
        else {
            throw new FilterException("is not a timestamp: " + value);
        }
    }
}
