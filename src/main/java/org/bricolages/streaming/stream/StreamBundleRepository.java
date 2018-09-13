package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.slf4j.Logger;
import lombok.*;

public interface StreamBundleRepository extends JpaRepository<StreamBundle, Long> {
    List<StreamBundle> findByStreamAndBucketAndPrefix(PacketStream stream, String bucket, String prefix);

    default StreamBundle findStreamBundle(PacketStream stream, String bucket, String prefix) {
        val list = findByStreamAndBucketAndPrefix(stream, bucket, prefix);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple stream bundle matched: " + stream.id + ", " + prefix);
        }
        return list.get(0);
    }

    List<StreamBundle> findByBucketAndPrefix(String bucket, String prefix);

    default StreamBundle findStreamBundle(String bucket, String prefix) {
        val list = findByBucketAndPrefix(bucket, prefix);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple stream bundle matched: bucket=" + bucket + ", prefix=" + prefix);
        }
        return list.get(0);
    }

    default StreamBundle findOrCreate(PacketStream stream, String bucket, String prefix, Logger log) {
        StreamBundle bundle = findStreamBundle(stream, bucket, prefix);
        if (bundle == null) {
            try {
                bundle = new StreamBundle(stream, bucket, prefix);
                save(bundle);
                logNewStreamBundle(log, stream.getId(), prefix);
            }
            catch (DataIntegrityViolationException ex) {
                bundle = findStreamBundle(stream, bucket, prefix);
            }
        }
        return bundle;
    }

    default void logNewStreamBundle(Logger log, long streamId, String streamPrefix) {
        log.warn("new stream bundle: stream_id={}, stream_prefix={}", streamId, streamPrefix);
    }
}
