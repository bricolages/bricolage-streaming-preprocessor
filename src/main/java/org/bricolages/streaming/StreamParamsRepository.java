package org.bricolages.streaming;
import org.bricolages.streaming.filter.TableId;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface StreamParamsRepository extends JpaRepository<StreamParams, Long> {
    List<StreamParams> findByStreamName(String streamName);

    default StreamParams findParams(TableId id) {
        val list = findByStreamName(id.toString());
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + id);
        }
        return list.get(0);
    }
}
