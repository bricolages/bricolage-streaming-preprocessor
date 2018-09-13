package org.bricolages.streaming.stream;
import org.bricolages.streaming.table.TargetTable;
import org.bricolages.streaming.exception.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import java.util.List;
import org.slf4j.Logger;
import lombok.*;

public interface PacketStreamRepository extends JpaRepository<PacketStream, Long> {
    List<PacketStream> findByStreamName(String streamName);

    default PacketStream findStream(String streamName) {
        val list = findByStreamName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + streamName);
        }
        return list.get(0);
    }

    default PacketStream createForce(String streamName, TargetTable table, Logger log) {
        try {
            // create new stream with disabled (to avoid to produce non preprocessed output)
            val stream = new PacketStream(streamName, table);
            save(stream);
            logNewStream(log, stream.getId(), streamName);
            return stream;
        }
        catch (DataIntegrityViolationException ex) {
            val stream = findStream(streamName);
            if (stream == null) {
                throw new ApplicationError("[FATAL] could not get stream: " + streamName);
            }
            return stream;
        }
    }

    default void logNewStream(Logger log, long streamId, String streamName) {
        log.warn("new stream: stream_id={}, stream_name={}", streamId, streamName);
    }
}
