package org.bricolages.streaming.filter;
import lombok.*;

public interface SequencialNumberAllocator {
    SequencialNumber allocate();
}
