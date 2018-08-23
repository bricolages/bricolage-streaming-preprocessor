package org.bricolages.streaming.stream.op;
import lombok.*;

public interface SequencialNumberAllocator {
    SequencialNumber allocate();
}
