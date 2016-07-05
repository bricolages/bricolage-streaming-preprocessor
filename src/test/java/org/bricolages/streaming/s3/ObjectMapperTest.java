package org.bricolages.streaming.s3;
import java.util.Arrays;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectMapperTest {
    ObjectMapper newMapper(ObjectMapper.Entry... entries) {
        return new ObjectMapper(Arrays.asList(entries));
    }

    ObjectMapper.Entry entry(String src, String dest) {
        return new ObjectMapper.Entry(src, dest);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void test_map() throws Exception {
        val map = newMapper(entry("s3://src-bucket/src-prefix/(.*\\.gz)", "s3://dest-bucket/dest-prefix/$1"));
        map.check();
        val result = map.map(loc("s3://src-bucket/src-prefix/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/datafile.json.gz"), result.getDestLocation());
        //FIXME: assertEquals(new TableId("tmp.table"), result.getTableId());
        assertNull(map.map(loc("s3://src-bucket-2/src-prefix/datafile.json.gz")));
    }
}
