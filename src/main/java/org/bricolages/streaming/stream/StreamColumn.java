package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.processor.StreamColumnProcessor;
import org.bricolages.streaming.stream.processor.ProcessorParams;
import org.bricolages.streaming.stream.processor.ProcessorContext;
import org.bricolages.streaming.exception.*;
import java.sql.Timestamp;
import javax.persistence.*;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@AllArgsConstructor
@Slf4j
@Entity
@Table(name="strload_columns", uniqueConstraints=@UniqueConstraint(columnNames={"stream_id", "column_name"}))
public class StreamColumn implements ProcessorParams {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="column_id", nullable=false)
    @Getter
    long id;

    @Column(name="stream_id", nullable=false)
    long streamId;

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

    @Column(name="time_unit", nullable=true)
    @Getter
    String timeUnit;

    public String getSourceName() {
        return (sourceName == null ? name : sourceName);
    }

    /* For tests */
    static public StreamColumn forName(String name) {
        return new StreamColumn(-1, -1, name, null, "dummy_type", null, null, null, null, null);
    }

    /* For tests */
    static public StreamColumn forNames(String name, String sourceName) {
        return new StreamColumn(-1, -1, name, sourceName, "dummy_type", null, null, null, null, null);
    }

    static public StreamColumn forParams(Params params) {
        return new StreamColumn(
            params.id,
            params.streamId,
            params.name,
            params.sourceName,
            params.type,
            params.length,
            params.sourceOffset,
            params.zoneOffset,
            params.createTime,
            params.timeUnit
        );
    }

    @NoArgsConstructor
    static public final class Params {
        public long id = -1;
        public long streamId = -1;
        public String name = null;
        public String sourceName = null;
        public String type = null;
        public Integer length = null;
        public String sourceOffset = null;
        public String zoneOffset = null;
        public Timestamp createTime = null;
        public String timeUnit = null;
    }

    public StreamColumnProcessor buildProcessor(ProcessorContext ctx) {
        return materializedType().getProcessorBuilder().apply(this, ctx);
    }

    StreamColumnType materializedType() {
        try {
            return StreamColumnType.intern(type);
        }
        catch(IllegalArgumentException ex) {
            throw new ConfigError("could not intern type name: " + type);
        }
    }
}
