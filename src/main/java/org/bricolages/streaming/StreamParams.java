package org.bricolages.streaming;

import java.util.List;
import javax.persistence.*;
import org.bricolages.streaming.filter.OperatorDefinition;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="strload_streams")
public class StreamParams {
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

    @Column(name="disabled")
    @Getter
    boolean disabled;

    @Column(name="discard")
    boolean discard;

    @Column(name="no_dispatch")
    boolean no_dispatch;

    public boolean doesDiscard() {
        return this.discard;
    }

    public boolean doesNotDispatch() {
        return this.no_dispatch;
    }
}