package org.bricolages.streaming.stream;
import org.bricolages.streaming.table.TargetTable;
import org.bricolages.streaming.object.S3ObjectLocator;
import org.bricolages.streaming.object.S3ObjectMetadata;
import org.bricolages.streaming.object.ObjectIOException;
import org.bricolages.streaming.exception.*;
import java.nio.file.Paths;
import lombok.*;

@AllArgsConstructor
public class Route {
    final PacketFilterFactory filterFactory;
    @Getter final PacketStream stream;
    final StreamBundle bundle;
    @Getter final String objectPrefix;
    @Getter final String objectName;

    public Route(PacketStream stream, StreamBundle bundle, String objectPrefix, String objectName) {
        this(null, stream, bundle, objectPrefix, objectName);
    }

    static public Route makeBlackhole() {
        return new Route(null, null, null, null, null);
    }

    public boolean isBlackhole() {
        return (objectName == null);
    }

    public String getStreamName() {
        if (stream == null) return null;
        return stream.getStreamName();
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

    public String getBucket() {
        return bundle.getBucket();
    }

    public String getPrefix() {
        return bundle.getPrefix();
    }

    public TargetTable getTable() {
        return stream.getTable();
    }

    public String getDestBucket() {
        return stream.getTable().getBucket();
    }

    public String getDestPrefix() {
        return stream.getTable().getPrefix();
    }

    public S3ObjectLocator getLocator() {
        if (bundle == null) return null;
        return new S3ObjectLocator(bundle.getBucket(), Paths.get(bundle.getPrefix(), objectPrefix, objectName).toString());
    }

    public S3ObjectLocator getDestLocator() {
        if (stream == null) return null;
        val table = stream.getTable();
        if (table == null) return null;
        return new S3ObjectLocator(table.getBucket(), Paths.get(table.getPrefix(), objectPrefix, objectName).toString());
    }

    public PacketFilter loadFilter() {
        return filterFactory.load(this);
    }

    public PacketFilterResult applyFilter() throws ObjectIOException, ConfigError {
        return loadFilter().processLocator(getLocator(), getDestLocator());
    }
}
