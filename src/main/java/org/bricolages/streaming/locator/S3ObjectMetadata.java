package org.bricolages.streaming.locator;
import com.amazonaws.services.s3.model.ObjectMetadata;
import java.util.Date;
import java.time.Instant;
import lombok.*;

@RequiredArgsConstructor
public class S3ObjectMetadata {
    final S3ObjectLocator locator;
    final ObjectMetadata meta;

    public S3ObjectMetadata(S3ObjectLocator loc, Instant createdTime, long size, String eTag) {
        this(loc, makeObjectMetadata(createdTime, size, eTag));
    }

    // For tests
    public S3ObjectMetadata(String url, Instant createdTime, long size, String eTag) throws LocatorParseException {
        this(S3ObjectLocator.parse(url), makeObjectMetadata(createdTime, size, eTag));
    }

    static ObjectMetadata makeObjectMetadata(Instant createdTime, long size, String eTag) {
        val meta = new ObjectMetadata();
        meta.setLastModified(new Date(createdTime.toEpochMilli()));
        meta.setContentLength(size);
        meta.setHeader("ETag", eTag);
        return meta;
    }

    public String bucket() {
        return locator.bucket();
    }

    public String key() {
        return locator.key();
    }

    public Instant createdTime() {
        Date d = meta.getLastModified();
        return Instant.ofEpochMilli(d.getTime());
    }

    public long size() {
        return meta.getContentLength();
    }

    public String eTag() {
        return meta.getETag();
    }
}
