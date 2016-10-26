package org.bricolages.streaming;
import javax.persistence.*;
import java.sql.Timestamp;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@Entity
@Table(name="preproc_incoming_streams")
class IncomingStream {
    @Id
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    @Column(name="stream_id")
    @Getter
    Long id;

    @Column(name="stream_name")
    String name;

    @Column(name="create_time")
    Timestamp createTime;

    public IncomingStream(String name) {
        this.name = name;
        this.createTime = new Timestamp(System.currentTimeMillis());
    }
}
