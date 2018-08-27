package org.bricolages.streaming.preproc;
import org.bricolages.streaming.stream.Packet;
import org.bricolages.streaming.util.SQLUtils;
import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Set;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_preproc_jobs")
public class PreprocJob {
    @Id
    @Column(name="preproc_job_id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Column(name="preproc_message_id")
    long preprocMessageId;

    @Getter
    @ManyToOne(optional=false)
    @JoinColumn(name="packet_id", nullable=false, updatable=false)
    Packet packet;

    @Column(name="start_time")
    Timestamp startTime = null;

    @Column(name="end_time")
    Timestamp endTime = null;

    @Column(name="status")
    String status = STATUS_STARTED;

    static final String STATUS_STARTED = "started";
    static final String STATUS_SUCCESS = "success";
    static final String STATUS_FAILURE = "failure";

    @Column(name="message")
    String message = "";

    public PreprocJob(PreprocMessage msg, Packet packet) {
        this.preprocMessageId = msg.getId();
        this.startTime = SQLUtils.currentTimestamp();
        this.packet = packet;
    }

    public void changeStateToSucceeded() {
        this.status = STATUS_SUCCESS;
        this.endTime = SQLUtils.currentTimestamp();
        this.message = "";
    }

    public void changeStateToFailed(String msg) {
        this.status = STATUS_FAILURE;
        this.endTime = SQLUtils.currentTimestamp();
        this.message = msg;
    }
}
