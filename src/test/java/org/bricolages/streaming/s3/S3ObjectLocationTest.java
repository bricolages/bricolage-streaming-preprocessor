package org.bricolages.streaming.s3;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class S3ObjectLocationTest {
    @Test
    public void testForUrl() throws Exception {
        val loc = S3ObjectLocation.forUrl("s3://my-bucket/my-prefix/object.txt");
        assertEquals("my-bucket", loc.getBucket());
        assertEquals("my-prefix/object.txt", loc.getKey());
    }

    @Test(expected = S3UrlParseException.class)
    public void testForUrl_parseError() throws Exception {
        S3ObjectLocation.forUrl("junk");
    }
}
