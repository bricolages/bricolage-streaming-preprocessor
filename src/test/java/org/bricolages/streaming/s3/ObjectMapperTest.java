package org.bricolages.streaming.s3;
import org.bricolages.streaming.ConfigError;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectMapperTest {
    ObjectMapper newMapper(ObjectMapper.Entry... entries) {
        return new ObjectMapper(Arrays.asList(entries));
    }

    ObjectMapper.Entry entry(String srcUrlPattern, String streamName, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
        return new ObjectMapper.Entry(srcUrlPattern, streamName, destBucket, streamPrefix, objectPrefix, objectName);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void map() throws Exception {
        val map = newMapper(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        val result = map.map("s3://src-bucket/src-prefix/schema.table/datafile.json.gz");
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(map.map("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz"));
    }

    @Test
    public void map_localfile() throws Exception {
        val map = newMapper(entry("file:/(?:.+/)?src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        val result = map.map("file:/path/to/src-bucket/src-prefix/schema.table/datafile.json.gz");
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(map.map("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz"));
    }
    
    @Test(expected=ConfigError.class)
    public void map_baddest() throws Exception {
        val map = newMapper(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$3", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        map.map("s3://src-bucket/src-prefix/schema.table/datafile.json.gz");
    }

    @Test(expected=ConfigError.class)
    public void map_badregex() throws Exception {
        val map = newMapper(entry("****", "$3", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
    }
}
