package org.bricolages.streaming;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import lombok.*;

public interface StreamBundleRepository extends JpaRepository<StreamBundle, Long> {
    List<StreamBundle> findByStreamAndBucketAndPrefix(DataStream stream, String bucket, String prefix);

    default StreamBundle findStreamBundle(DataStream stream, String bucket, String prefix) {
        val list = findByStreamAndBucketAndPrefix(stream, bucket, prefix);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple stream bundle matched: " + stream.id + ", " + prefix);
        }
        return list.get(0);
    }
}
