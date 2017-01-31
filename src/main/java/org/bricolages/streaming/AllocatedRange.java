package org.bricolages.streaming;

import java.util.Optional;
import lombok.*;

public class AllocatedRange {
    private long nextValue;
    private final long upperValue;

    public AllocatedRange(long nextValue, long upperValue) {
        this.nextValue = nextValue;
        this.upperValue = upperValue;
    }

    public static AllocatedRange none() {
        return new AllocatedRange(0, -1);
    }

    public boolean isRemaining() {
        return this.nextValue < this.upperValue;
    }

    public Optional<Long> getNextValue() {
        if (!isRemaining()) {
            return Optional.empty();
        }
        val ret = Optional.of(this.nextValue);
        this.nextValue++;
        return ret;
    }
}