package org.bricolages.streaming.s3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import java.nio.file.Path;
import java.nio.file.Paths;
import lombok.*;

@Getter
@EqualsAndHashCode(of={"urlString"})
public class S3ObjectLocation {
    static public S3ObjectLocation forUrl(String url) throws S3UrlParseException {
        try {
            val u = new AmazonS3URI(url);
            return new S3ObjectLocation(u.getBucket(), u.getKey());
        }
        catch (IllegalArgumentException ex) {
            throw new S3UrlParseException("could not parse S3 URL: " + url);
        }
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

    public String basename() {
        return Paths.get(key).getFileName().toString();
    }

    public GetObjectRequest newGetRequest() {
        return new GetObjectRequest(bucket, key);
    }

    public PutObjectRequest newPutRequest(Path src) {
        return new PutObjectRequest(bucket, key, src.toFile());
    }

    public boolean isGzip() {
        return key.endsWith(".gz");
    }
}
