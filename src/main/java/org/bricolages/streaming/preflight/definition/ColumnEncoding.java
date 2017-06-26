package org.bricolages.streaming.preflight.definition;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.*;

@RequiredArgsConstructor
public enum ColumnEncoding {
    ZSTD("zstd"), LZO("lzo"), DELTA("delta"), RAW("raw");

    final private String expression;

    public String toString() {
        return expression;
    }

    @JsonCreator
    public static ColumnEncoding forValue(String expression) {
        for (val value : ColumnEncoding.values()) {
            if (value.toString().equals(expression)) {
                return value;
            }
        }
        throw new IllegalStateException("no encodings matched");
    }
}
