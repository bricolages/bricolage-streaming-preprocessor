package org.bricolages.streaming;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface StreamParamsRepository extends JpaRepository<StreamParams, Long> {
    List<StreamParams> findByStreamName(String streamName);

    default StreamParams findParams(String streamName) {
        val list = findByStreamName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + streamName);
        }
        return list.get(0);
    }
}
