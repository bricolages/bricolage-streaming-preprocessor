package org.bricolages.streaming.filter;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import lombok.*;

public interface SequencialNumberRepository extends JpaRepository<SequencialNumber, Long> {
    @Query(value = "select last_value, nextval('strload_sequence') from strload_sequence", nativeQuery = true)
    SequencialNumber allocate();
}
