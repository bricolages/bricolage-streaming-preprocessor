package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import java.util.Date;
import java.util.Set;
import java.util.List;
import java.sql.Timestamp;
import lombok.*;

public interface StreamColumnRepository extends JpaRepository<StreamColumn, Long> {
    public default List<StreamColumn> findColumns(PacketStream stream) {
        return findByStreamIdOrderById(stream.getId());
    }

    List<StreamColumn> findByStreamIdOrderById(long streamId);

    public default void saveUnknownColumns(PacketStream stream, Set<String> names) {
        for (String name : names) {
            saveUnknownColumn(stream, name);
        }
    }

    public default StreamColumn saveUnknownColumn(PacketStream stream, String name) {
        val params = new StreamColumn.Params();
        params.streamId = stream.getId();
        params.name = name;
        params.sourceName = null;
        params.type = "unknown";
        params.createTime = new Timestamp(new Date().getTime());
        val column = StreamColumn.forParams(params);
        try {
            return save(column);
        }
        catch (DataIntegrityViolationException ex) {
            return null;
        }
    }
}
