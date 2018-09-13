package org.bricolages.streaming.table;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import lombok.*;

public interface ChunkRepository extends JpaRepository<Chunk, Long> {
    Chunk findByObjectUrl(String url);

    default public Chunk upsert(Chunk chunk) {
        val curr = findByObjectUrl(chunk.getObjectUrl());
        if (curr != null) {
            curr.merge(chunk);
            return save(curr);
        }
        try {
            return save(chunk);
        }
        catch (DataIntegrityViolationException ex) {
            val curr2 = findByObjectUrl(chunk.getObjectUrl());
            curr2.merge(chunk);
            return save(curr2);
        }
    }
}
