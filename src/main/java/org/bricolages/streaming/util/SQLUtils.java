package org.bricolages.streaming.util;
import java.sql.Timestamp;
import java.time.Instant;
import lombok.*;

public final class SQLUtils {
    private SQLUtils() {}

    static public Timestamp currentTimestamp() {
        return new Timestamp(System.currentTimeMillis());
    }

    static public Timestamp getTimestamp(Instant t) {
        return new Timestamp(t.toEpochMilli());
    }
}
