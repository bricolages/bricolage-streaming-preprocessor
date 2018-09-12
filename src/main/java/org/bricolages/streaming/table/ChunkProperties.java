package org.bricolages.streaming.table;
import java.time.Instant;

public interface ChunkProperties {
    int getObjectRows();
    int getErrorRows();
    String getObjectUrl();
    long getObjectSize();
    Instant getObjectCreatedTime();
}
