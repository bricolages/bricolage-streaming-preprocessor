package org.bricolages.streaming.stream.processor;
import java.util.Set;
import lombok.*;

public class NullContext implements ProcessorContext {
    public Set<String> getStreamColumns() {
        return null;
    }
}
