package org.bricolages.streaming.stream;
import org.bricolages.streaming.object.S3ObjectMetadata;
import java.time.Instant;
import java.util.Set;
import java.util.HashSet;
import lombok.*;

@NoArgsConstructor
public class PacketFilterResult {
    @Getter
    int inputRows = 0;

    @Getter
    int outputRows = 0;

    @Getter
    int errorRows = 0;

    @Getter
    @Setter
    S3ObjectMetadata objectMetadata = null;

    public String getObjectUrl() {
        return objectMetadata.url();
    }

    public Instant getObjectCreatedTime() {
        return objectMetadata.createdTime();
    }

    public long getObjectSize() {
        return objectMetadata.size();
    }

    final Set<String> unknownColumns = new HashSet<String>();

    public void addUnknownColumn(String name) {
        this.unknownColumns.add(name);
    }

    public Set<String> getUnknownColumns() {
        return this.unknownColumns;
    }
}
