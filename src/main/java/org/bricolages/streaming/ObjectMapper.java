package org.bricolages.streaming;
import java.util.List;
import lombok.*;

@AllArgsConstructor
public class ObjectMapper {
    final List<Entry> entries;

    public S3ObjectLocation map(S3ObjectLocation src) {
        // FIXME: implement
        return src;
    }

    public static final class Entry {
        public String src;
        public String dest;
    }
}
