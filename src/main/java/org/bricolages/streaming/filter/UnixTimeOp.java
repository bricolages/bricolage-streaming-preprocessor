package org.bricolages.streaming.filter;
import java.time.*;
import lombok.*;

class UnixTimeOp extends Op {
    final ZoneOffset zoneOffset;

    public UnixTimeOp(String offset) {
        this.zoneOffset = ZoneOffset.of(offset);
    }

    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        return formatSqlTimestamp(unixTimeToOffsetDateTime(getInteger(value), zoneOffset));
    }
}
