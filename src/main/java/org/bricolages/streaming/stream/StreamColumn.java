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

    @Column(name="column_type", nullable=false)
    @Getter
    String type;

    @Column(name="source_name", nullable=true)
    @Getter
    String sourceName;

    /* Regexp pattern */
    @Column(name="source_name_pattern", nullable=true)
    @Getter
    String sourceNamePattern;

    @Column(name="value_length", nullable=true)
    @Getter
    int length;

    @Column(name="value_precision", nullable=true)
    @Getter
    int precision;

    @Column(name="source_offset", nullable=true)
    @Getter
    String sourceOffset;

    @Column(name="zone_offset", nullable=true)
    @Getter
    String zoneOffset;

    @Column(name="create_time", nullable=false)
    Timestamp createTime;

    /* For tests */
    public StreamColumn(String name, String type) {
        this(0, null, name, type, null, null, 0, 0, null, null, null);
    }
}
