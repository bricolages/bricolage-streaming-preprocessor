package org.bricolages.streaming;

import java.sql.Timestamp;
import java.util.List;
import javax.persistence.*;
import org.bricolages.streaming.filter.OperatorDefinition;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_streams")
public class DataStream {
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

    public boolean doesDiscard() {
        return this.discard;
    }

    public boolean doesNotDispatch() {
        return this.no_dispatch;
    }

    public boolean doesDefer() {
        return this.disabled || !this.initialized;
    }

    public DataStream(String streamName) {
        this.streamName = streamName;
        this.createTime = new Timestamp(System.currentTimeMillis());
    }
}