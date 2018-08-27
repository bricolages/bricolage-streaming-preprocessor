package org.bricolages.streaming.preproc;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PreprocMessageRepository extends JpaRepository<PreprocMessage, Long> {
    PreprocMessage findByMessageId(String msgid);
}
