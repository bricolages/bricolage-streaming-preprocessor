package org.bricolages.streaming.s3;
import org.bricolages.streaming.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;
import lombok.*;

@DataJpaTest
@RunWith(SpringJUnit4ClassRunner.class)
public class ObjectMapperTest {
    ObjectMapper newRouter(ObjectMapper.Entry... entries) {
        val router = new ObjectMapper(Arrays.asList(entries));
        router.streamRepos = streamRepos;
        router.streamBundleRepos = bundleRepos;
        return router;
    }

    ObjectMapper.Entry entry(String srcUrlPattern, String streamName, String streamPrefix, String destBucket, String destPrefix, String objectPrefix, String objectName) {
        return new ObjectMapper.Entry(srcUrlPattern, streamName, streamPrefix, destBucket, destPrefix, objectPrefix, objectName);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void mapByPatterns() throws Exception {
        val map = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        val result = map.mapByPatterns("s3://src-bucket/src-prefix/schema.table/datafile.json.gz");
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(map.mapByPatterns("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz"));
    }

    @Test
    public void map_localfile() throws Exception {
        val map = newRouter(entry("file:/(?:.+/)?src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        val result = map.mapByPatterns("file:/path/to/src-bucket/src-prefix/schema.table/datafile.json.gz");
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(map.mapByPatterns("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz"));
    }
    
    @Test(expected=ConfigError.class)
    public void map_baddest() throws Exception {
        val map = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$3", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
        map.mapByPatterns("s3://src-bucket/src-prefix/schema.table/datafile.json.gz");
    }

    @Test(expected=ConfigError.class)
    public void map_badregex() throws Exception {
        val map = newRouter(entry("****", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        map.check();
    }

    @Autowired TestEntityManager entityManager;
    @Autowired DataStreamRepository streamRepos;
    @Autowired StreamBundleRepository bundleRepos;

    @Test
    public void map() throws Exception {
        entityManager.persist(new DataStream("schema.table"));
        val stream = streamRepos.findStream("schema.table");
        entityManager.persist(new StreamBundle(stream, "src-bucket-2", "src-prefix-2", "dest-bucket-2", "dest-prefix-2"));
        val bundle = bundleRepos.findStreamBundle("src-bucket-2", "src-prefix-2");

        val router = newRouter(entry("file:/(?:.+/)?src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();

        val result = router.map(loc("s3://src-bucket-2/src-prefix-2/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket-2/dest-prefix-2/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());

        assertNull(router.map(loc("s3://src-bucket-UNKNOWN/src-prefix-UNKNOWN/schema.table/datafile.json.gz")));
        assertNull(router.map(loc("s3://src-bucket-2/datafile.json.gz")));
    }
}
