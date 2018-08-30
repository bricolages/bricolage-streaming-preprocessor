package org.bricolages.streaming.preproc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import lombok.*;

public interface PreprocMessageRepository extends JpaRepository<PreprocMessage, Long> {
    PreprocMessage findByMessageId(String msgid);

    default public PreprocMessage upsert(PreprocMessage msg) {
        // to reduce duplicated rows error
        val curr = findByMessageId(msg.getMessageId());
        if (curr != null) return curr;
        try {
            return save(msg);
        }
        catch (DataIntegrityViolationException ex) {
            // no need to update
            return findByMessageId(msg.getMessageId());
        }
    }
}
