package org.bricolages.streaming.stream;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.exception.*;
import java.nio.file.Paths;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class BoundStream {
    final ObjectFilterFactory filterFactory;
    @Getter final PacketStream stream;
    @Getter final StreamBundle bundle;
    @Getter final String objectPrefix;
    @Getter final String objectName;

    public BoundStream(PacketStream stream, StreamBundle bundle, String objectPrefix, String objectName) {
        this(null, stream, bundle, objectPrefix, objectName);
    }

    static public BoundStream makeBlackhole() {
        return new BoundStream(null, null, null, null, null);
    }

    public boolean isBlackhole() {
        return (objectName == null);
    }

    public S3ObjectLocator getDestLocator() {
        if (stream == null) return null;
        if (bundle == null) return null;
        return new S3ObjectLocator(bundle.getDestBucket(), Paths.get(bundle.getDestPrefix(), objectPrefix, objectName).toString());
    }

    public String getStreamName() {
        if (stream == null) return null;
        return stream.getStreamName();
    }

    public boolean doesDefer() {
        return stream.doesDefer();
    }

    public boolean doesDiscard() {
        return stream.doesDiscard();
    }

    public boolean doesNotDispatch() {
        return stream.doesNotDispatch();
    }

    public ObjectFilter loadFilter() {
        return filterFactory.load(stream);
    }

    public S3ObjectMetadata processLocator(S3ObjectLocator src, S3ObjectLocator dest, FilterResult result) throws LocatorIOException, ConfigError {
        return loadFilter().processLocator(src, dest, result, stream.getStreamName());
    }
}
