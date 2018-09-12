package org.bricolages.streaming.table;
import javax.persistence.*;
import lombok.*;

@NoArgsConstructor
@Entity
@Table(name="strload_tables")
public class TargetTable {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="table_id")
    @Getter
    long id;

    @Column(name="schema_name")
    @Getter
    String schemaName;

    @Column(name="table_name")
    @Getter
    String tableName;

    @Column(name="s3_bucket")
    @Getter
    String bucket;

    @Column(name="s3_prefix")
    @Getter
    String prefix;

    @Column(name="disabled")
    boolean disabled = false;

    public TargetTable(String schemaName, String tableName, String bucket, String prefix) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.bucket = bucket;
        this.prefix = prefix;
    }
}
