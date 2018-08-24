package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ChunkRepository extends JpaRepository<Chunk, Long> {
    Chunk findByObjectUrl(String url);
}
