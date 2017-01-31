package org.bricolages.streaming;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.transaction.annotation.Transactional;
import java.util.List;
import lombok.*;

public interface SequencialNumberRepository extends JpaRepository<SequencialNumber, Long> {
    List<SequencialNumber> findById(long id);

    default SequencialNumber findSequence(long id) {
        val list = findById(id);
        if (list.isEmpty())
            return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + id);
        }
        return list.get(0);
    }

    @Transactional
    default AllocatedRange allocate(long id, long size) {
        SequencialNumber num = findSequence(id);
        long nextValue = num.value;
        num.value += size;
        save(num);

        return new AllocatedRange(nextValue, num.value);
    }
}