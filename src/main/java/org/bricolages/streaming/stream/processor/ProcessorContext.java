package org.bricolages.streaming.stream.processor;
import java.util.Set;
import lombok.*;

public interface ProcessorContext {
    Set<String> getStreamColumns();
}
