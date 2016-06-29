package org.bricolages.streaming;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;

public class ObjectMapperTest {
    ObjectMapper newMapper(ObjectMapper.Entry... entries) {
        return new ObjectMapper(Arrays.asList(entries));
    }

    ObjectMapper.Entry entry(String src, String dest) {
        return new ObjectMapper.Entry(src, dest);
    }

    S3ObjectLocation loc(String url) {
        return S3ObjectLocation.forUrl(url);
    }

    @Test public void testMap() {
        ObjectMapper map = newMapper(entry("s3://src-bucket/src-prefix/(.*\\.gz)", "s3://dest-bucket/dest-prefix/$1"));
        map.check();
        assertEquals(loc("s3://dest-bucket/dest-prefix/datafile.json.gz"), map.map(loc("s3://src-bucket/src-prefix/datafile.json.gz")));
        assertNull(map.map(loc("s3://src-bucket-2/src-prefix/datafile.json.gz")));
    }
}
