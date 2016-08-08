package org.bricolages.streaming;
import org.bricolages.streaming.filter.TableId;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="preproc_tables")
class TableParams {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Getter
    long id;

    @Column(name="table_id")
    String tableId;

    @Column(name="disabled")
    @Getter
    boolean disabled;

    public TableParams(TableId id) {
        this.tableId = id.toString();
    }

    public TableId getTableId() {
        return TableId.parse(tableId);
    }
}
