package org.bricolages.streaming.stream.processor;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;
import java.util.concurrent.TimeUnit;

import lombok.*;

public final class Cleanse {
    private Cleanse() {}

    static public boolean isInteger(Object value) {
        if (value instanceof Integer || value instanceof Long || value instanceof Short || value instanceof Byte) {
            return true;
        }
        else if (value instanceof Float || value instanceof Double) {
            return false;
        }
        else if (value instanceof String) {
            try {
                Long.valueOf((String)value);
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

    static public long getInteger(Object value) throws CleanseException {
        if (value instanceof Integer) {
            return ((Integer)value).longValue();
        }
        else if (value instanceof Long) {
            return ((Long)value).longValue();
        }
        else if (value instanceof Short) {
            return ((Short)value).longValue();
        }
        else if (value instanceof Byte) {
            return ((Byte)value).longValue();
        }
        else if (value instanceof String) {
            try {
                return Long.valueOf((String)value);
            }
            catch (NumberFormatException ex) {
                throw new CleanseException(ex);
            }
        }
        else if (value instanceof Float) {
            return ((Float)value).longValue();
        }
        else if (value instanceof Double) {
            return ((Double)value).longValue();
        }
        else {
            throw new CleanseException("unexpected value for integer");
        }
    }

    // Returns true if value is a float.
    // If it seems integers, this method returns false.
    static public boolean isFloat(Object value) {
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

    static public float getFloat(Object value) throws CleanseException {
        if (value instanceof Integer) {
            return ((Integer)value).floatValue();
        }
        else if (value instanceof Long) {
            return ((Long)value).floatValue();
        }
        else if (value instanceof Short) {
            return ((Short)value).floatValue();
        }
        else if (value instanceof Byte) {
            return ((Byte)value).floatValue();
        }
        else if (value instanceof String) {
            try {
                return Float.valueOf((String)value);
            }
            catch (NumberFormatException ex) {
                throw new CleanseException(ex);
            }
        }
        else if (value instanceof Float) {
            return ((Float)value).floatValue();
        }
        else if (value instanceof Double) {
            return ((Double)value).floatValue();
        }
        else {
            throw new CleanseException("unexpected value for float");
        }
    }

    static public double getDouble(Object value) throws CleanseException {
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
                throw new CleanseException(ex);
            }
        }
        else if (value instanceof Float) {
            return ((Float)value).doubleValue();
        }
        else if (value instanceof Double) {
            return ((Double)value).doubleValue();
        }
        else {
            throw new CleanseException("unexpected value for double");
        }
    }

    static public OffsetDateTime getLocalOffsetDateTime(Object value, ZoneOffset sourceOffset, ZoneOffset zoneOffset, TimeUnit unit) throws CleanseException {
        if (isFloat(value)) {
            return unixTimeToOffsetDateTime(getDouble(value), zoneOffset, unit);
        }
        else if (isInteger(value)) {
            return unixTimeToOffsetDateTime(getInteger(value), zoneOffset, unit);
        }
        else if (value instanceof String) {
            return getOffsetDateTime(value, sourceOffset).withOffsetSameInstant(zoneOffset);
        }
        else {
            throw new CleanseException("is not an integer, a float or a string: " + value);
        }
    }

    static public OffsetDateTime unixTimeToOffsetDateTime(long t, ZoneOffset offset, TimeUnit unit) throws CleanseException {
        try {
            switch (unit) {
            case SECONDS:
                return Instant.ofEpochSecond(t).atOffset(offset);
            case MILLISECONDS:
                return Instant.ofEpochMilli(t).atOffset(offset);
            default:
                throw new RuntimeException("BUG: unsupported time unit for unix time: " + unit);
            }
        }
        catch (DateTimeException ex) {
            throw new CleanseException(ex);
        }
    }

    static public OffsetDateTime unixTimeToOffsetDateTime(double t, ZoneOffset offset, TimeUnit unit) throws CleanseException {
        try {
            switch (unit) {
            case SECONDS:
                return Instant.ofEpochMilli((long)(t * 1000.0)).atOffset(offset);
            case MILLISECONDS:
                return Instant.ofEpochMilli((long)t).atOffset(offset);
            default:
                throw new RuntimeException("BUG: unsupported time unit for unix time: " + unit);
            }
        }
        catch (DateTimeException ex) {
            throw new CleanseException(ex);
        }
    }

    static final DateTimeFormatter SQL_DATE_FORMAT = DateTimeFormatter.ISO_DATE;

    static public String formatSqlDate(LocalDate dt) throws CleanseException {
        try {
            return dt.format(SQL_DATE_FORMAT);
        }
        catch (DateTimeException ex) {
            throw new CleanseException(ex);
        }
    }

    // Redshift does not support timezone, but COPY just ignores zone.
    // So keeping timezone information in the JSON data file does not hurt loading task and is a good thing.
    static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    static public String formatSqlTimestamp(OffsetDateTime dt) throws CleanseException {
        try {
            return dt.format(SQL_TIMESTAMP_FORMAT);
        }
        catch (DateTimeException ex) {
            throw new CleanseException(ex);
        }
    }

    static final Pattern TIMESTAMP_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)? ?(?:Z|\\w{1,5}|[+-]\\d{2}:?\\d{2})?");

    static public OffsetDateTime getOffsetDateTime(Object value, ZoneOffset defaultOffset) throws CleanseException {
        if (! (value instanceof String)) {
            throw new CleanseException("is not a string: " + value);
        }
        String strValue = ((String)value).trim();
        val m = TIMESTAMP_PATTERN.matcher(strValue);
        if (!m.find()) {
            throw new CleanseException("is not a timestamp: " + strValue);
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
        throw new CleanseException("could not parse a timestamp: " + str);
    }

    static final DateTimeFormatter ISO_INSTANT_2 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss[.SSS][ ]xxxx");

    /* "2016-07-01T12:34:56Z": Fluentd default.  This representation appears most frequently.
     * ISO_INSTANT supports milliseconds
     */
    static public OffsetDateTime tryParsingIsoInstant(String str) {
        try {
            return OffsetDateTime.parse(str, DateTimeFormatter.ISO_INSTANT);
        }
        catch (DateTimeException e) {
            try {
                return OffsetDateTime.parse(str, ISO_INSTANT_2);
            }
            catch (DateTimeException e2) {
                return null;
            }
        }
    }

    /* "2016-07-01T12:34:56+00:00": Some log file have this.  Old Fluentd format?
     * ISO_OFFSET_DATE_TIME supports milliseconds
     */
    static public OffsetDateTime tryParsingIsoOffsetDateTime(String str) {
        try {
            return OffsetDateTime.parse(str, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }
        catch (DateTimeException e) {
            return null;
        }
    }

    static final DateTimeFormatter RUBY_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][ ]xxx");
    static final DateTimeFormatter RUBY_DATE_TIME_2 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS][ ]xxxx");
    static final DateTimeFormatter RUBY_DATE_TIME_NOTZ = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");

    /* "2016-07-01 12:34:56 +0000": Ruby Time#to_s
     */
    static public OffsetDateTime tryParsingRubyDateTime(String str) {
        try {
            return OffsetDateTime.parse(str, RUBY_DATE_TIME);
        }
        catch (DateTimeException e1) {
            try {
                return OffsetDateTime.parse(str, RUBY_DATE_TIME_2);
            }
            catch (DateTimeException e2) {
                return null;
            }
        }
    }

    static final DateTimeFormatter RAILS_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss z");
    static final DateTimeFormatter RAILS_DATE_TIME_FRAC = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS z");

    /* "2016-07-01 12:34:56 UTC": Rails TimeWithZone#to_s
     */
    static public OffsetDateTime tryParsingRailsDateTime(String str) {
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
    static public OffsetDateTime tryParsingIsoDateTime(String str, ZoneOffset defaultOffset) {
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

    static public LocalDate getLocalDate(Object value) throws CleanseException {
        return getLocalDate(value, ZoneOffset.UTC, null);
    }

    static public LocalDate getLocalDate(Object value, ZoneOffset defaultOffset, ZoneOffset targetOffset) throws CleanseException {
        if (! (value instanceof String)) {
            throw new CleanseException("is not a string");
        }
        val str = (String)value;
        try {
            return LocalDate.parse(str, DateTimeFormatter.ISO_DATE);
        }
        catch (DateTimeException e) {
            val dt = getOffsetDateTime(str, defaultOffset);
            return (targetOffset != null ? dt.withOffsetSameInstant(targetOffset) : dt).toLocalDate();
        }
    }
}
