package org.bricolages.streaming.event;
import org.junit.Test;
import com.amazonaws.services.sqs.model.Message;
import static org.junit.Assert.*;
import lombok.*;

public class S3EventTest {
    @Test
    public void isCopyEvent() throws Exception {
        val e = parseEvent("{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"bricolage:preprocessor\",\"eventTime\":\"2016-09-06T13:52:49Z\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"s3SchemaVersion\":\"1.0\",\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"test-prefix/test-file.json.gz\",\"size\":\"1358\",\"eTag\":\"bfa868a9c00376f6704f41d6a9e0da20\"}}}]}");
        assertEquals(false, e.isCopyEvent());

        val e2 = parseEvent("{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"bricolage:preprocessor\",\"eventTime\":\"2016-09-06T13:52:49Z\",\"eventName\":\"ObjectCreated:Copy\",\"s3\":{\"s3SchemaVersion\":\"1.0\",\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"test-prefix/test-file.json.gz\",\"size\":\"1358\",\"eTag\":\"bfa868a9c00376f6704f41d6a9e0da20\"}}}]}");
        assertEquals(true, e2.isCopyEvent());
    }

    S3Event parseEvent(String body) throws Exception {
        val msg = new Message();
        msg.setBody(body);
        return S3Event.forMessage(msg);
    }
}
