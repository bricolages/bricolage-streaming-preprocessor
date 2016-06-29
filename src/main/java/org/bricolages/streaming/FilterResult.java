package org.bricolages.streaming;
import lombok.AllArgsConstructor;

@AllArgsConstructor
class FilterResult {
    static FilterResult empty() {
        return new FilterResult(0, 0, 0);
    }

    long inputLines;
    long outputLines;
    long jsonParseError;
}
