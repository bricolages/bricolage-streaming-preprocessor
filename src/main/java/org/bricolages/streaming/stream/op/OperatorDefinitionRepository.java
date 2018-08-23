package org.bricolages.streaming.stream.op;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface OperatorDefinitionRepository extends JpaRepository<OperatorDefinition, Long> {
}
