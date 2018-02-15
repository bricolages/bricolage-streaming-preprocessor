package org.bricolages.streaming.stream;
import java.sql.Timestamp;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Entity
@Table(name="strload_columns")
public class StreamColumn {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="column_id")
    @Getter
    long id;

    @ManyToOne
    @JoinColumn(name="stream_id", nullable=false)
    @Getter
    PacketStream stream;

    @Column(name="column_name", nullable=false)
    @Getter
    String name;

    @Column(name="source_name", nullable=true)
    String sourceName;

    @Column(name="value_type", nullable=false)
    @Getter
    String type;

    @Column(name="value_length", nullable=true)
    @Getter
    Integer length;

    @Column(name="source_offset", nullable=true)
    @Getter
    String sourceOffset;

    @Column(name="zone_offset", nullable=true)
    @Getter
    String zoneOffset;

    @Column(name="create_time", nullable=false)
    Timestamp createTime;

    public String getSourceName() {
        return (sourceName == null ? name : sourceName);
    }

    /* For tests */
    static public StreamColumn forName(String name) {
        return new StreamColumn(-1, null, name, null, "dummy_type", null, null, null, null);
    }

    /* For tests */
    static public StreamColumn forNames(String name, String sourceName) {
        return new StreamColumn(-1, null, name, sourceName, "dummy_type", null, null, null, null);
    }

    /* For tests */
    static public StreamColumn forParams(Params params) {
        return new StreamColumn(
            params.id,
            params.stream,
            params.name,
            params.sourceName,
            params.type,
            params.length,
            params.sourceOffset,
            params.zoneOffset,
            params.createTime
        );
    }

    /* For tests */
    @NoArgsConstructor
    static public final class Params {
        public long id = -1;
        public PacketStream stream = null;
        public String name = null;
        public String sourceName = null;
        public String type = null;
        public Integer length = null;
        public String sourceOffset = null;
        public String zoneOffset = null;
        public Timestamp createTime = null;
    }
}
