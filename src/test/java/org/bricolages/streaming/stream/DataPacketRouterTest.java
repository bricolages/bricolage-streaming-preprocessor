package org.bricolages.streaming.stream;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.s3.*;
import org.bricolages.streaming.exception.*;
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
public class DataPacketRouterTest {
    DataPacketRouter newRouter(DataPacketRouter.Entry... entries) {
        val router = new DataPacketRouter(Arrays.asList(entries));
        router.streamRepos = streamRepos;
        router.streamBundleRepos = bundleRepos;
        return router;
    }

    DataPacketRouter.Entry entry(String srcUrlPattern, String streamName, String streamPrefix, String destBucket, String destPrefix, String objectPrefix, String objectName) {
        return new DataPacketRouter.Entry(srcUrlPattern, streamName, streamPrefix, destBucket, destPrefix, objectPrefix, objectName);
    }

    S3ObjectLocation loc(String url) throws S3UrlParseException {
        return S3ObjectLocation.forUrl(url);
    }

    @Test
    public void routeByPatterns() throws Exception {
        val router = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
        val result = router.routeWithoutDB(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertNotNull(result);
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(router.routeWithoutDB(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }

    /* FIXME: temporary off: We should not try to route local file
    @Test
    public void route_localfile() throws Exception {
        val router = newRouter(entry("file:/(?:.+/)?src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
        val result = router.routeWithoutDB(loc("file:/path/to/src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocation());
        assertEquals("schema.table", result.getStreamName());
        assertNull(router.routeWithoutDB(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }
    */
    
    @Test(expected=ConfigError.class)
    public void route_baddest() throws Exception {
        val router = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$3", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
        router.routeWithoutDB(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
    }

    @Test(expected=ConfigError.class)
    public void route_badregex() throws Exception {
        val router = newRouter(entry("****", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
    }

    @Autowired TestEntityManager entityManager;
    @Autowired DataStreamRepository streamRepos;
    @Autowired StreamBundleRepository bundleRepos;

    @Test
    public void route() throws Exception {
        entityManager.persist(new DataStream("schema.table"));
        val stream = streamRepos.findStream("schema.table");
        entityManager.persist(new StreamBundle(stream, "src-bucket", "0000.schema.table_2", "dest-bucket-2", "dest-prefix-2"));
        val bundle = bundleRepos.findStreamBundle("src-bucket-2", "src-prefix-2");

        val router = newRouter(entry("s3://src-bucket/(0000.(schema\\.table))/(2017/11/28)/(.*\\.gz)", "$2", "$1", "dest-bucket", "dest/$1", "$2", "$3"));
        router.check();

        val result = router.route(loc("s3://src-bucket/0000.schema.table_2/2017/11/28/datafile.json.gz"));
        assertNotNull(result);
        assertEquals("schema.table", result.getStreamName());
        assertEquals(loc("s3://dest-bucket-2/dest-prefix-2/2017/11/28/datafile.json.gz"), result.getDestLocation());

        assertNull(router.route(loc("s3://src-bucket-UNKNOWN/src-prefix-UNKNOWN/schema.table/datafile.json.gz")));
        assertNull(router.route(loc("s3://src-bucket-2/datafile.json.gz")));
    }
}
