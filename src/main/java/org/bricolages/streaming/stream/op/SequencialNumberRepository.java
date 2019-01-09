package org.bricolages.streaming.stream.op;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import lombok.*;

public interface SequencialNumberRepository extends JpaRepository<SequencialNumber, Long> {
    // CLUDGE: provide dummy last_value to fill all fields
    @Query(value = "select nextval('strload_sequence'), -1 as last_value", nativeQuery = true)
    SequencialNumber _allocateNext();

    default SequencialNumber allocate() {
        val num = _allocateNext();
        num.setLastValueFromNextValue();
        return num;
    }
}
