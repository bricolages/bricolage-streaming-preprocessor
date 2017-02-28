package org.bricolages.streaming.preflight;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.*;

@RequiredArgsConstructor
public enum ColumnEncoding {
    LZO("lzo"), RAW("raw");

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
