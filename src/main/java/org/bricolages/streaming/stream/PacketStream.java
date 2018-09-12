package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.op.OperatorDefinition;
import org.bricolages.streaming.table.TargetTable;
import org.bricolages.streaming.util.SQLUtils;
import java.sql.Timestamp;
import java.util.List;
import javax.persistence.*;
import lombok.*;

@NoArgsConstructor
@Entity
@Table(name="strload_streams")
public class PacketStream {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="stream_id")
    @Getter
    long id;

    @Column(name="stream_name")
    @Getter
    String streamName;

    @OneToMany(mappedBy="stream", fetch=FetchType.EAGER)
    @OrderBy("application_order asc")
    @Getter
    List<OperatorDefinition> operatorDefinitions;

    @OneToMany(mappedBy="stream", fetch=FetchType.LAZY)
    @Getter
    List<StreamBundle> bundles;

    @OneToOne(optional=true, fetch=FetchType.EAGER)
    @JoinColumn(name="table_id", nullable=true, unique=true)
    @Getter
    @Setter
    TargetTable table;

    @Column(name="disabled")
    boolean disabled;

    @Column(name="discard")
    boolean discard;

    @Column(name="no_dispatch")
    boolean no_dispatch;

    @Column(name="initialized")
    boolean initialized;

    @Column(name="create_time")
    Timestamp createTime;

    @Column(name="column_initialized", nullable=false)
    boolean columnInitialized;

    public boolean doesDiscard() {
        return this.discard;
    }

    public boolean doesNotDispatch() {
        return this.no_dispatch;
    }

    public boolean isNotInitialized() {
        return !this.initialized;
    }

    public boolean isDisabled() {
        return this.disabled;
    }

    public PacketStream(String streamName) {
        this.streamName = streamName;
        this.createTime = SQLUtils.currentTimestamp();
    }

    public PacketStream(String streamName, TargetTable table) {
        this.streamName = streamName;
        this.createTime = SQLUtils.currentTimestamp();
        this.table = table;
    }

    public boolean doesUseColumn() {
        return this.columnInitialized;
    }
}
