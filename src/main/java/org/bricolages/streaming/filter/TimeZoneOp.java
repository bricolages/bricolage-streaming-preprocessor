package org.bricolages.streaming.filter;
import java.time.ZoneOffset;
import lombok.*;

class TimeZoneOp extends Op {
    final ZoneOffset sourceOffset;
    final ZoneOffset targetOffset;

    public TimeZoneOp(String sourceOffset, String targetOffset) {
        this.sourceOffset = ZoneOffset.of(sourceOffset);
        this.targetOffset = ZoneOffset.of(targetOffset);
    }

    @Override
    public Object apply(Object value) throws FilterException {
        if (value == null) return null;
        return formatSqlTimestamp(getOffsetDateTime(value, sourceOffset).withOffsetSameInstant(targetOffset));
    }
}
