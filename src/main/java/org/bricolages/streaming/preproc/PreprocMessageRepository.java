package org.bricolages.streaming.preproc;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;

public interface PreprocMessageRepository extends JpaRepository<PreprocMessage, Long> {
    PreprocMessage findByMessageId(String msgid);

    default public PreprocMessage upsert(PreprocMessage msg) {
        try {
            return save(msg);
        }
        catch (DataIntegrityViolationException ex) {
            // no need to update
            return findByMessageId(msg.getMessageId());
        }
    }
}
