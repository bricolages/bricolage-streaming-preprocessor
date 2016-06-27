package org.bricolages.streaming;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.model.Message;
import java.io.IOException;
import lombok.*;

@Getter
class S3Event extends Event {
    static public S3Event forMessage(Message msg) throws IOException {
        S3EventNotification body = S3EventNotification.parseJson(msg.getBody());
        // FIXME: check record size
        S3EventNotification.S3EventNotificationRecord rec = body.getRecords().get(0);
        return new S3Event(
            msg,
            new S3ObjectLocation(rec.getS3().getBucket().getName(), rec.getS3().getObject().getKey()),
            rec.getS3().getObject().getSizeAsLong()
        );
    }

    final S3ObjectLocation location;
    final long objectSize;

    public S3Event(Message msg, S3ObjectLocation location, long objectSize) {
        super(msg);
        this.location = location;
        this.objectSize = objectSize;
    }
}
