package org.bricolages.streaming;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.AmazonClientException;
import java.io.IOException;
import lombok.*;

@Getter
public class S3Event extends Event {
    static public final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return msg.getBody().contains("\"eventName\":\"ObjectCreated:");
        }

        @Override
        public Event parse(Message msg) throws MessageParseException {
            return S3Event.forMessage(msg);
        }
    }

    static public S3Event forMessage(Message msg) throws MessageParseException {
        try {
            S3EventNotification body = S3EventNotification.parseJson(msg.getBody());
            // FIXME: check record size
            S3EventNotification.S3EventNotificationRecord rec = body.getRecords().get(0);
            return new S3Event(
                msg,
                new S3ObjectLocation(
                    rec.getS3().getBucket().getName(),
                    rec.getS3().getObject().getKey()
                ),
                rec.getS3().getObject().getSizeAsLong(),
                rec
            );
        }
        catch (AmazonClientException ex) {
            throw new MessageParseException("S3 event message parse error: " + ex.getMessage());
        }
    }

    final S3ObjectLocation location;
    final long objectSize;
    final S3EventNotification.S3EventNotificationRecord record;

    S3Event(Message msg, S3ObjectLocation location, long objectSize, S3EventNotification.S3EventNotificationRecord record) {
        super(msg);
        this.location = location;
        this.objectSize = objectSize;
        this.record = record;
    }

    void callHandler(EventHandlers h) {
        h.handleS3Event(this);
    }
}
