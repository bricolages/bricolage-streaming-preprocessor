package org.bricolages.streaming.event;
import org.bricolages.streaming.object.*;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.model.Message;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.amazonaws.AmazonClientException;
import org.joda.time.DateTime;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.time.Instant;
import java.util.HashMap;
import java.util.Objects;
import lombok.*;

@Getter
public class S3Event extends Event {
    static final class Parser implements MessageParser {
        boolean isBareS3Message(String body) {
            return body.contains("\"eventName\":\"ObjectCreated:") && body.contains("\"s3\":{");
        }

        boolean isSnsWrappedS3Message(String body) {
            return body.contains("\\\"eventName\\\":\\\"ObjectCreated:") && body.contains("\\\"s3\\\":{");
        }

        @Override
        public boolean isCompatible(Message msg) {
            val body = msg.getBody();
            return isBareS3Message(body) || isSnsWrappedS3Message(body);
        }

        @Override
        public S3Event parse(Message msg) throws MessageParseException {
            val body = msg.getBody();
            if (isSnsWrappedS3Message(body)) {
                val mapper = new ObjectMapper();
                val typeRef = new TypeReference<HashMap<String, String>>() {};
                try {
                    HashMap<String, String> map = mapper.readValue(body, typeRef);
                    // set message body to unwraped message
                    msg.setBody(map.get("Message"));
                } catch (IOException ex) {
                    throw new MessageParseException(ex);
                }
            }
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
            if (rec.getS3() == null || rec.getS3().getObject() == null) {
                throw new MessageParseException("S3 event message parse error: missing .s3 object");
            }
            return new S3Event(
                msg,
                rec.getEventName(),
                new S3ObjectLocator(
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

    final String eventName;
    final S3ObjectLocator locator;
    final long objectSize;
    final S3EventNotification.S3EventNotificationRecord record;
    final boolean noDispatch;

    S3Event(Message msg, String eventName, S3ObjectLocator locator, Long objectSize, S3EventNotification.S3EventNotificationRecord record, boolean noDispatch) {
        super(msg);
        this.eventName = eventName;
        this.locator = locator;
        this.objectSize = objectSize == null ? 0 : objectSize;
        this.record = record;
        this.noDispatch = noDispatch;
    }

    public S3ObjectMetadata getObjectMetadata() {
        return new S3ObjectMetadata(locator, getEventTime(), objectSize, getETag());
    }

    public Instant getEventTime() {
        return Instant.ofEpochMilli(record.getEventTime().getMillis());
    }

    public String getETag() {
        return record.getS3().getObject().geteTag();
    }

    public void callHandler(EventHandlers h) {
        h.handleS3Event(this);
    }

    public boolean isCopyEvent() {
        return Objects.equals(eventName, "ObjectCreated:Copy");
    }

    public boolean doesNotDispatch() {
        return noDispatch;
    }

    @Override
    public String toString() {
        return "#<S3Event " + eventName + " messageId=" + getMessageId() + ", object=" + locator + ">";
    }
}
