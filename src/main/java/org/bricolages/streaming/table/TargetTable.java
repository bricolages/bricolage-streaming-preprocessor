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

    @Column(name="load_batch_size")
    @Getter
    int loadBatchSize;

    @Column(name="load_interval")
    @Getter
    int loadInterval;   // in seconds

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
        this.loadBatchSize = DEFAULT_LOAD_BATCH_SIZE;
        this.loadInterval = randomInitialLoadInterval();
    }

    static final int DEFAULT_LOAD_BATCH_SIZE = 2400;

    static final int DEFAULT_LOAD_INTERVAL_BASE = 6 * 60 * 60;   // 6 hours
    static final int DEFAULT_LOAD_INTERVAL_MIN = (int)(DEFAULT_LOAD_INTERVAL_BASE * 0.95);
    static final int DEFAULT_LOAD_INTERVAL_MAX = (int)(DEFAULT_LOAD_INTERVAL_BASE * 1.05);

    /**
     * Returns 6 hours +/- 5%.
     * Fixed load interval causes too many concurrent loading, we must introduce random factor.
     */
    static int randomInitialLoadInterval() {
        double randomFactor = 1 + (Math.random() - 0.5) / 10;   // [0.95, 1.05)
        return (int)(DEFAULT_LOAD_INTERVAL_BASE * randomFactor);
    }
}
