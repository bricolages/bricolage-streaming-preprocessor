package org.bricolages.streaming;
import org.bricolages.streaming.filter.TableId;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.List;
import lombok.*;

interface TableParamsRepository extends JpaRepository<TableParams, Long> {
    List<TableParams> findByTableId(String tableId);

    default TableParams findParams(TableId id) {
        val list = findByTableId(id.toString());
        if (list.isEmpty()) return null;
        if (list.size() > 1) {
            throw new ApplicationError("FATAL: multiple table parameters matched: " + id);
        }
        return list.get(0);
    }
}
