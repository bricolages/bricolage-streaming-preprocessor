package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.hibernate.Hibernate;
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
        val bundle = list.get(0);
        Hibernate.initialize(bundle.getStream());
        return bundle;
    }
}
