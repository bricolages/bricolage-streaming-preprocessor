package org.bricolages.streaming.event;
import org.bricolages.streaming.s3.S3ObjectLocation;
import org.bricolages.streaming.s3.S3ObjectMetadata;
import lombok.*;

@RequiredArgsConstructor
public class FakeS3Event extends LogQueueEvent {
    final S3ObjectMetadata object;

    @Override
    public String messageBody() {
        val w = new SQSMessageBodyWriter();
        w.beginObject();
            w.beginArray("Records");
                w.beginObject();
                    w.pair("eventVersion", "2.0");
                    w.pair("eventSource", "bricolage:preprocessor");
                    w.pair("eventTime", object.createdTime());
                    w.pair("eventName", "ObjectCreated:Put");
                    w.beginObject("s3");
                        w.pair("s3SchemaVersion", "1.0");
                        w.beginObject("bucket");
                            w.pair("name", object.bucket());
                        w.endObject();
                        w.beginObject("object");
                            w.pair("key", object.key());
                            w.pair("size", object.size());
                            if (object.eTag() != null)
                                w.pair("eTag", object.eTag());
                        w.endObject();
                    w.endObject();
                w.endObject();
            w.endArray();
        w.endObject();
        return w.toString();
    }
}
