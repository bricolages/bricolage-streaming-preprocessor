package org.bricolages.streaming.table;
import org.bricolages.streaming.exception.*;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TargetTableRepository extends JpaRepository<TargetTable, Long> {
    TargetTable findBySchemaNameAndTableName(String schemaName, String tableName);

    default TargetTable findTable(String schemaName, String tableName) {
        return findBySchemaNameAndTableName(schemaName, tableName);
    }
}
