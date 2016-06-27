package org.bricolages.streaming;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.*;

@AllArgsConstructor
@Getter
class S3ObjectLocation {
    final String bucket;
    final String key;

    public Path basename() {
        return Paths.get(key).getFileName();
    }

    public GetObjectRequest getRequest() {
        return new GetObjectRequest(bucket, key);
    }

    public boolean isGzip() {
        return Paths.get(key).endsWith(".gz");
    }
}
