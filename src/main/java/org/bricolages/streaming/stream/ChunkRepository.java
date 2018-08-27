package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import lombok.*;

public interface ChunkRepository extends JpaRepository<Chunk, Long> {
    Chunk findByObjectUrl(String url);

    default public Chunk upsert(Chunk chunk) {
        try {
            return save(chunk);
        }
        catch (DataIntegrityViolationException ex) {
            val newChunk = chunk;
            val oldChunk = findByObjectUrl(newChunk.getObjectUrl());
            oldChunk.merge(newChunk);
            return save(oldChunk);
        }
    }
}
