package org.bricolages.streaming.preproc;
import org.bricolages.streaming.stream.PacketStream;
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

    @Column(name="stream_id")
    Long streamId = null;

    @Column(name="packet_id")
    Long packetId = null;

    @Getter
    @Column(name="handled")
    boolean handled = false;

    public PreprocMessage(String messageId, S3ObjectMetadata obj) {
        this(messageId, SQLUtils.getTimestamp(obj.createdTime()), SQLUtils.currentTimestamp(), obj.url());
    }

    public PreprocMessage(String messageId, Timestamp eventTime, Timestamp receivedTime, String objectUrl) {
        this.messageId = messageId;
        this.eventTime = eventTime;
        this.receivedTime = receivedTime;
        this.objectUrl = objectUrl;
    }

    public void changeStateToStreamDetected(PacketStream stream) {
        this.streamId = stream.getId();
    }

    public void changeStateToJobStarted(Packet packet) {
        this.packetId = packet.getId();
    }

    public void changeStateToHandled() {
        this.handled = true;
    }
}
