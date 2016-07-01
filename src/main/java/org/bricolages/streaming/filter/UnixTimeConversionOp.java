package org.bricolages.streaming.filter;
import java.time.*;
import lombok.*;

class UnixTimeConversionOp extends Op {
    final ZoneOffset zoneOffset;

    public UnixTimeConversionOp(String offset) {
        this.zoneOffset = ZoneOffset.of(offset);
    }

    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        return formatSqlTimestamp(unixTimeToOffsetDateTime(getInteger(value), zoneOffset));
    }
}
