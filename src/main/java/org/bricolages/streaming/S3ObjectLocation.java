package org.bricolages.streaming;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.*;

@Getter
@EqualsAndHashCode(of={"urlString"})
public class S3ObjectLocation {
    static public S3ObjectLocation forUrl(String url) {
        val u = new AmazonS3URI(url);
        return new S3ObjectLocation(u.getBucket(), u.getKey());
    }

    final String bucket;
    final String key;
    final String urlString;

    public S3ObjectLocation(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        this.urlString = "s3://" + bucket + "/" + key;
    }

    public String toString() {
        return urlString();
    }

    public String urlString() {
        return this.urlString;
    }

    public Path basename() {
        return Paths.get(key).getFileName();
    }

    public GetObjectRequest getRequest() {
        return new GetObjectRequest(bucket, key);
    }

    public boolean isGzip() {
        return key.endsWith(".gz");
    }
}
