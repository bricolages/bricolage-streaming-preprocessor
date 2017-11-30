package org.bricolages.streaming.locator;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class S3ObjectLocatorTest {
    @Test
    public void test_parse() throws Exception {
        val loc = S3ObjectLocator.parse("s3://my-bucket/my-prefix/object.txt");
        assertEquals("my-bucket", loc.getBucket());
        assertEquals("my-prefix/object.txt", loc.getKey());
    }

    @Test(expected = LocatorParseException.class)
    public void test_parse_err() throws Exception {
        S3ObjectLocator.parse("junk");
    }
}
