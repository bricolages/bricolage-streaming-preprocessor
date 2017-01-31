package org.bricolages.streaming;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import lombok.*;

public interface SequencialNumberRepository extends JpaRepository<SequencialNumber, Long> {
    @Query(value = "select sequence_name, last_value, nextval('preproc_sequence') from preproc_sequence", nativeQuery = true)
    SequencialNumber allocate();
}
