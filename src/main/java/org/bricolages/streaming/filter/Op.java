package org.bricolages.streaming.filter;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.Instant;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import lombok.*;

abstract class Op {
    abstract Object apply(Object value) throws FilterException;

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

    static final DateTimeFormatter SQL_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    protected String formatSqlTimestamp(OffsetDateTime dt) throws FilterException {
        try {
            return dt.format(SQL_TIMESTAMP_FORMAT);
        }
        catch (DateTimeException ex) {
            throw new FilterException(ex);
        }
    }
}
