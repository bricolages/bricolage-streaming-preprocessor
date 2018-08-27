package org.bricolages.streaming.preproc;
import org.bricolages.streaming.stream.Packet;
import org.bricolages.streaming.object.S3ObjectMetadata;
import org.bricolages.streaming.util.SQLUtils;
import javax.persistence.*;
import java.sql.Timestamp;
import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Entity
@Table(name="strload_preproc_messages")
public class PreprocMessage {
    @Getter
    @Id
    @Column(name="preproc_message_id")
    @GeneratedValue(strategy=GenerationType.IDENTITY)
    long id;

    @Getter
    @Column(name="message_id")
    String messageId;

    @Getter
    @Column(name="event_time")
    Timestamp eventTime;

    @Getter
    @Column(name="received_time")
    Timestamp receivedTime;

    @Getter
    @Column(name="object_url")
    String objectUrl;

    @Getter
    @Setter
    @ManyToOne(optional=true, fetch=FetchType.LAZY)
    @JoinColumn(name="packet_id")
    Packet packet;

    @Getter
    @Column(name="handled")
    boolean handled = false;

    public PreprocMessage(String messageId, S3ObjectMetadata obj) {
        this(messageId, SQLUtils.getTimestamp(obj.createdTime()), SQLUtils.currentTimestamp(), obj.url(), null);
    }

    public PreprocMessage(String messageId, Timestamp eventTime, Timestamp receivedTime, String objectUrl, Packet packet) {
        this.messageId = messageId;
        this.eventTime = eventTime;
        this.receivedTime = receivedTime;
        this.objectUrl = objectUrl;
        this.packet = packet;
    }

    public void changeStateToHandled() {
        this.handled = true;
    }
}
