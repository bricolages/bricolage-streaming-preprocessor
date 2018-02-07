package org.bricolages.streaming.filter;
import java.time.*;
import java.util.regex.Pattern;
import java.time.format.DateTimeFormatter;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

@Slf4j
public abstract class Op {
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

    // Returns true if value is a float.
    // If it seems integers, this method returns false.
    protected boolean isFloat(Object value) {
        if (value instanceof Double) {
            return true;
        }
        else if (value instanceof Float) {
            return true;
        }
        else if (value instanceof String) {
            try {
                Double.valueOf((String)value);
                return true;
            }
            catch (NumberFormatException ex) {
                return false;
            }
        }
        else {
            return false;
        }
    }

    protected double getFloat(Object value) throws FilterException {
        if (value instanceof Integer) {
            return ((Integer)value).doubleValue();
        }
        else if (value instanceof Long) {
            return ((Long)value).doubleValue();
        }
        else if (value instanceof String) {
            try {
                return Double.valueOf((String)value);
            }
            catch (NumberFormatException ex) {
                throw new FilterException(ex);
            }
        }
        else if (value instanceof Float) {
            return ((Float)value).doubleValue();
        }
        else if (value instanceof Double) {
            return ((Double)value).doubleValue();
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

    protected OffsetDateTime unixTimeToOffsetDateTime(double t, ZoneOffset offset) throws FilterException {
        try {
            return Instant.ofEpochMilli((long)(t * 1000)).atOffset(offset);
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

    static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)? ?(?:Z|\\w{1,5}|[+-]\\d{2}:?\\d{2})?");

    protected OffsetDateTime getOffsetDateTime(Object value, ZoneOffset defaultOffset, boolean truncate) throws FilterException {
        if (! (value instanceof String)) {
            throw new FilterException("is not a string: " + value);
        }
        String strValue = ((String)value).trim();
        val m = TIMESTAMP_PATTERN.matcher(strValue);
        if (!m.find()) {
            throw new FilterException("is not a timestamp: " + strValue);
        }
        String str = m.group();
        // Order DOES matter
        val t1 = tryParsingIsoInstant(str);
        if (t1 != null) return t1;
        val t2 = tryParsingIsoOffsetDateTime(str);
        if (t2 != null) return t2;
        val t3 = tryParsingRubyDateTime(str);
        if (t3 != null) return t3;
        val t4 = tryParsingRailsDateTime(str);
        if (t4 != null) return t4;
        val t5 = tryParsingIsoDateTime(str, defaultOffset);
        if (t5 != null) return t5;
        throw new FilterException("could not parse a timestamp: " + str);
    }

    /* "2016-07-01T12:34:56Z": Fluentd default.  This representation appears most
     * ISO_INSTANT supports milliseconds
     */
    protected OffsetDateTime tryParsingIsoInstant(String str) {
        try {
            return OffsetDateTime.parse(str, DateTimeFormatter.ISO_INSTANT);
        }
        catch (DateTimeException e) {
            return null;
        }
    }

    /* "2016-07-01T12:34:56+00:00": Some log file have this.  Old Fluentd format?
     * ISO_OFFSET_DATE_TIME supports milliseconds
     */
    protected OffsetDateTime tryParsingIsoOffsetDateTime(String str) {
        try {
            return OffsetDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        catch (DateTimeException e) {
            return null;
        }
    }

    static protected final DateTimeFormatter RUBY_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss xxxx");
    static protected final DateTimeFormatter RUBY_DATE_TIME_NOTZ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    static protected final DateTimeFormatter RUBY_DATE_TIME_FRAC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS xxxx");

    /* "2016-07-01 12:34:56 +0000": Ruby Time#to_s
     */
    protected OffsetDateTime tryParsingRubyDateTime(String str) {
        try {
            return OffsetDateTime.parse(str, RUBY_DATE_TIME);
        }
        catch (DateTimeException e1) {
            try {
                return OffsetDateTime.parse(str, RUBY_DATE_TIME_FRAC);
            }
            catch (DateTimeException e2) {
                return null;
            }
        }
    }

    static protected final DateTimeFormatter RAILS_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    static protected final DateTimeFormatter RAILS_DATE_TIME_FRAC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

    /* "2016-07-01 12:34:56 UTC": Rails TimeWithZone#to_s
     */
    protected OffsetDateTime tryParsingRailsDateTime(String str) {
        try {
            return ZonedDateTime.parse(str, RAILS_DATE_TIME).toOffsetDateTime();
        }
        catch (DateTimeException e1) {
            try {
                return ZonedDateTime.parse(str, RAILS_DATE_TIME_FRAC).toOffsetDateTime();
            }
            catch (DateTimeException e2) {
                return null;
            }
        }
    }

    /* "2016-07-01T12:34:56": ISO format but without offset.
     * This format means to lost time offset data, try this at last.
     */
    protected OffsetDateTime tryParsingIsoDateTime(String str, ZoneOffset defaultOffset) {
        try {
            return LocalDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME).atOffset(defaultOffset);
        }
        catch (DateTimeException e) {
            try {
                return LocalDateTime.parse(str, RUBY_DATE_TIME_NOTZ).atOffset(defaultOffset);
            }
            catch (DateTimeException e2) {
                return null;
            }
        }
    }
}
