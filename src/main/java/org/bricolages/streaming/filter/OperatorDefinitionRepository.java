package org.bricolages.streaming.filter;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

public interface OperatorDefinitionRepository extends JpaRepository<OperatorDefinition, Long> {
    List<OperatorDefinition> findByTargetTable(String targetTable);
}
