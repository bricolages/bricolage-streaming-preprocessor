package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.hibernate.Hibernate;
import java.util.List;
import lombok.*;

public interface PacketStreamRepository extends JpaRepository<PacketStream, Long> {
    List<PacketStream> findByStreamName(String streamName);

    default PacketStream findStream(String streamName) {
        val list = findByStreamName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + streamName);
        }
        val stream = list.get(0);
        if (stream.doesUseColumn()) {
            Hibernate.initialize(stream.getColumns());
        }
        return stream;
    }
}
