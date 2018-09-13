package org.bricolages.streaming.table;
import org.bricolages.streaming.util.SQLUtils;
import javax.persistence.*;
import java.sql.Timestamp;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_chunks")
public class Chunk {
    @Getter
    @Id
    @Column(name="chunk_id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Getter
    @Column(name="object_url")
    String objectUrl;

    @Getter
    @Column(name="object_size")
    long objectSize;

    @Getter
    @Column(name="object_rows")
    int objectRows;

    @Getter
    @Column(name="error_rows")
    int errorRows;

    @Getter
    @Column(name="object_created_time")
    Timestamp objectCreatedTime = null;

    @OneToOne
    @JoinColumn(name="table_id", unique=false, nullable=true)
    TargetTable table;

    @Column(name="dispatched")
    boolean dispatched;

    @Column(name="loaded")
    boolean loaded;

    public Chunk(TargetTable table, ChunkProperties props) {
        this(props.getObjectUrl(), props.getObjectSize(), props.getObjectRows(), props.getErrorRows(), SQLUtils.getTimestamp(props.getObjectCreatedTime()), table);
    }

    public Chunk(String objectUrl, long objectSize, int objectRows, int errorRows, Timestamp objectCreatedTime, TargetTable table) {
        this.objectUrl = objectUrl;
        this.objectSize = objectSize;
        this.objectRows = objectRows;
        this.errorRows = errorRows;
        this.objectCreatedTime = objectCreatedTime;
        this.table = table;
    }

    public void changeStateToDispatched() {
        this.dispatched = true;
    }

    public void merge(Chunk other) {
        this.objectSize = other.objectSize;
        this.objectRows = other.objectRows;
        this.errorRows = other.errorRows;
        this.objectCreatedTime = other.objectCreatedTime;
        this.table = other.table;
    }
}
