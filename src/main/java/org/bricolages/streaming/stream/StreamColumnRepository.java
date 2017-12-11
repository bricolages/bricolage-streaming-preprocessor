package org.bricolages.streaming.stream;
import org.springframework.data.jpa.repository.JpaRepository;
import lombok.*;

public interface StreamColumnRepository extends JpaRepository<StreamColumn, Long> {
}
