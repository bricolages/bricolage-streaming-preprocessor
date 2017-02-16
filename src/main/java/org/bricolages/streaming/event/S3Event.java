package org.bricolages.streaming.event;
import org.bricolages.streaming.s3.S3ObjectLocation;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.AmazonClientException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import lombok.*;

@Getter
public class S3Event extends Event {
    static final class Parser implements MessageParser {
        @Override
        public boolean isCompatible(Message msg) {
            return msg.getBody().contains("\"eventName\":\"ObjectCreated:")
                && msg.getBody().contains("\"s3\":{");
        }

        @Override
        public Event parse(Message msg) throws MessageParseException {
            return S3Event.forMessage(msg);
        }
    }

    static S3Event forMessage(Message msg) throws MessageParseException {
        try {
            S3EventNotification body = S3EventNotification.parseJson(msg.getBody());
            if (body.getRecords().size() != 1) {
                throw new MessageParseException("FATAL: SQS message record size is not 1" + body.getRecords().size());
            }
            S3EventNotification.S3EventNotificationRecord rec = body.getRecords().get(0);
            return new S3Event(
                msg,
                new S3ObjectLocation(
                    rec.getS3().getBucket().getName(),
                    URLDecoder.decode(rec.getS3().getObject().getKey(), "UTF-8")
                ),
                rec.getS3().getObject().getSizeAsLong(),
                rec,
                msg.getBody().contains("\"noDispatch\":true")   // CLUDGE, but there is no other handy way
            );
        }
        catch (AmazonClientException | UnsupportedEncodingException ex) {
            throw new MessageParseException("S3 event message parse error: " + ex.getMessage());
        }
    }

    final S3ObjectLocation location;
    final long objectSize;
    final S3EventNotification.S3EventNotificationRecord record;
    final boolean noDispatch;

    S3Event(Message msg, S3ObjectLocation location, long objectSize, S3EventNotification.S3EventNotificationRecord record, boolean noDispatch) {
        super(msg);
        this.location = location;
        this.objectSize = objectSize;
        this.record = record;
        this.noDispatch = noDispatch;
    }

    public void callHandler(EventHandlers h) {
        h.handleS3Event(this);
    }

    public boolean doesNotDispatch() {
        return noDispatch;
    }

    @Override
    public String toString() {
        return "#<S3Event messageId=" + getMessageId() + ", object=" + location + ">";
    }
}
