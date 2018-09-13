package org.bricolages.streaming.table;
import org.bricolages.streaming.exception.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.dao.DataIntegrityViolationException;
import org.slf4j.Logger;
import lombok.*;

public interface TargetTableRepository extends JpaRepository<TargetTable, Long> {
    TargetTable findBySchemaNameAndTableName(String schemaName, String tableName);

    default TargetTable findTable(String schemaName, String tableName) {
        return findBySchemaNameAndTableName(schemaName, tableName);
    }

    default TargetTable findOrCreate(String schemaName, String tableName, String dataSourceId, String bucket, String prefix, Logger log) {
        try {
            val table = new TargetTable(schemaName, tableName, dataSourceId, bucket, prefix);
            save(table);
            logNewTable(log, table.getId(), schemaName, tableName);
            return table;
        }
        catch (DataIntegrityViolationException ex) {
            val table = findTable(schemaName, tableName);
            if (table == null) {
                throw new ApplicationError("[FATAL] could not get table: " + schemaName + "." + tableName);
            }
            return table;
        }
    }

    default void logNewTable(Logger log, long tableId, String schemaName, String tableName) {
        log.warn("new table: table_id={}, table_name={}.{}", tableId, schemaName, tableName);
    }
}
