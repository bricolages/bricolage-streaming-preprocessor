package org.bricolages.streaming.stream;
import org.bricolages.streaming.util.SQLUtils;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import java.util.List;
import lombok.*;

public interface StreamColumnRepository extends JpaRepository<StreamColumn, Long> {
    public default List<StreamColumn> findColumns(PacketStream stream) {
        return findByStreamIdOrderById(stream.getId());
    }

    List<StreamColumn> findByStreamIdOrderById(long streamId);

    public default StreamColumn saveUnknownColumn(PacketStream stream, String name) {
        val params = new StreamColumn.Params();
        params.streamId = stream.getId();
        params.name = name;
        params.sourceName = null;
        params.type = "unknown";
        params.createTime = SQLUtils.currentTimestamp();
        val column = StreamColumn.forParams(params);
        try {
            return save(column);
        }
        catch (DataIntegrityViolationException ex) {
            return null;
        }
    }
}
