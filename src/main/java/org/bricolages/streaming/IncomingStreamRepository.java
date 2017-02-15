package org.bricolages.streaming;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

interface IncomingStreamRepository extends JpaRepository<IncomingStream, Long> {
    List<IncomingStream> findByName(String name);

    default IncomingStream findStream(String streamName) {
        val list = findByName(streamName);
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + streamName);
        }
        return list.get(0);
    }
}
