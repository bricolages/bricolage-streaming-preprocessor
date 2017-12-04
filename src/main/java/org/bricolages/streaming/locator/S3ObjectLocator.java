package org.bricolages.streaming.locator;
import com.amazonaws.services.s3.AmazonS3URI;
import java.nio.file.Paths;
import lombok.*;

@Getter
@EqualsAndHashCode(of="urlString", callSuper=false)
public class S3ObjectLocator extends Locator {
    static public S3ObjectLocator parse(String url) throws LocatorParseException {
        try {
            val u = new AmazonS3URI(url);
            return new S3ObjectLocator(u.getBucket(), u.getKey());
        }
        catch (IllegalArgumentException ex) {
            throw new LocatorParseException("could not parse S3 URL: " + url);
        }
    }

    final String bucket;
    final String key;
    final String urlString;

    public S3ObjectLocator(String bucket, String key) {
        this.bucket = bucket;
        this.key = key;
        this.urlString = "s3://" + bucket + "/" + key;
    }

    @Override public String toString() { return this.urlString; }
    @Override public boolean isLocalFile() { return false; }
    @Override public boolean isS3Object() { return true; }
    @Override public S3ObjectLocator asS3ObjectLocator() { return this; }

    public String bucket() {
        return this.bucket;
    }

    public String key() {
        return this.key;
    }

    public String basename() {
        return Paths.get(key).getFileName().toString();
    }

    public boolean isGzip() {
        return key.endsWith(".gz");
    }
}
