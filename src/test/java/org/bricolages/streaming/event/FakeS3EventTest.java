package org.bricolages.streaming.event;
import org.bricolages.streaming.s3.S3ObjectMetadata;
import java.time.Instant;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class FakeS3EventTest {
    @Test
    public void messageBody() throws Exception {
        /*
            % echo '{"Records":[{"eventVersion":"2.0","eventSource":"bricolage:preprocessor","eventTime":"2016-09-06T13:52:49Z","eventName":"ObjectCreated:Put","s3":{"s3SchemaVersion":"1.0","bucket":{"name":"test-bucket"},"object":{"key":"test-prefix/test-file.json.gz","size":"1358","eTag":"bfa868a9c00376f6704f41d6a9e0da20"}}}]}' | ruby -rjson -e 'puts JSON.pretty_generate(JSON.load($stdin.read))'
            {
              "Records": [
                {
                  "eventVersion": "2.0",
                  "eventSource": "bricolage:preprocessor",
                  "eventTime": "2016-09-06T13:52:49Z",
                  "eventName": "ObjectCreated:Put",
                  "s3": {
                    "s3SchemaVersion": "1.0",
                    "bucket": {
                      "name": "test-bucket"
                    },
                    "object": {
                      "key": "test-prefix/test-file.json.gz",
                      "size": "1358",
                      "eTag": "bfa868a9c00376f6704f41d6a9e0da20"
                    }
                  }
                }
              ]
            }
        */
        val e = new FakeS3Event(new S3ObjectMetadata("s3://test-bucket/test-prefix/test-file.json.gz", Instant.parse("2016-09-06T13:52:49Z"), 1358, "bfa868a9c00376f6704f41d6a9e0da20"));
        assertEquals("{\"Records\":[{\"eventVersion\":\"2.0\",\"eventSource\":\"bricolage:preprocessor\",\"eventTime\":\"2016-09-06T13:52:49Z\",\"eventName\":\"ObjectCreated:Put\",\"s3\":{\"s3SchemaVersion\":\"1.0\",\"bucket\":{\"name\":\"test-bucket\"},\"object\":{\"key\":\"test-prefix/test-file.json.gz\",\"size\":\"1358\",\"eTag\":\"bfa868a9c00376f6704f41d6a9e0da20\"}}}]}", e.messageBody());
    }
}
