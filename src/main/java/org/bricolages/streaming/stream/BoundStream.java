package org.bricolages.streaming.stream;
import org.bricolages.streaming.object.S3ObjectLocator;
import org.bricolages.streaming.object.S3ObjectMetadata;
import org.bricolages.streaming.object.ObjectIOException;
import org.bricolages.streaming.exception.*;
import java.nio.file.Paths;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class BoundStream {
    final PacketFilterFactory filterFactory;
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

    public long getTableId() {
        return stream.getTableId();
    }

    public boolean isNotInitialized() {
        return stream.isNotInitialized();
    }

    public boolean isDisabled() {
        return stream.isDisabled();
    }

    public boolean doesDiscard() {
        return stream.doesDiscard();
    }

    public boolean doesNotDispatch() {
        return stream.doesNotDispatch();
    }

    public PacketFilter loadFilter() {
        return filterFactory.load(this);
    }

    public PacketFilterResult processLocator(S3ObjectLocator src, S3ObjectLocator dest) throws ObjectIOException, ConfigError {
        return loadFilter().processLocator(src, dest);
    }
}