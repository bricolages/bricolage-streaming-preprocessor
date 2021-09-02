package org.bricolages.streaming.stream;
import org.bricolages.streaming.object.*;
import org.bricolages.streaming.table.*;
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
public class PacketRouterTest {
    @Autowired TestEntityManager entityManager;
    @Autowired PacketFilterFactory filterFactory;
    @Autowired TargetTableRepository tableRepos;
    @Autowired PacketStreamRepository streamRepos;
    @Autowired StreamBundleRepository bundleRepos;

    PacketRouter newRouter(PacketRouter.Entry... entries) {
        val router = new PacketRouter(Arrays.asList(entries));
        router.filterFactory = filterFactory;
        router.tableRepos = tableRepos;
        router.streamRepos = streamRepos;
        router.bundleRepos = bundleRepos;
        return router;
    }

    PacketRouter.Entry entry(String srcUrlPattern, String streamName, String streamPrefix, String destBucket, String destPrefix, String objectPrefix, String objectName) {
        return new PacketRouter.Entry(srcUrlPattern, streamName, streamPrefix, destBucket, destPrefix, objectPrefix, objectName);
    }

    PacketRouter.Entry blackholeEntry(String pat) {
        val ent = new PacketRouter.Entry(pat, null, null, null, null, null, null);
        ent.setBlackhole(true);
        return ent;
    }

    S3ObjectLocator loc(String url) throws LocatorParseException {
        return S3ObjectLocator.parse(url);
    }

    @Test
    public void routeWithoutDB() throws Exception {
        val router = newRouter(entry("s3://src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
        val result = router.routeWithoutDB(loc("s3://src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertNotNull(result);
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocator());
        assertEquals("schema.table", result.getStreamName());
        assertNull(router.routeWithoutDB(loc("s3://src-bucket-2/src-prefix/schema.table/datafile.json.gz")));
    }

    @Test
    public void routeByPatterns_create() throws Exception {
        val router = newRouter(entry("s3://src-bucket/(\\w{4}\\.(?:\\w+\\.\\w+\\.)?(\\w+\\.\\w+))/(\\d{4}/\\d{2}/\\d{2})/(.*\\.gz)", "$2", "$1", "dest-bucket", "$1", "$3", "$4"));
        router.check();
        val result = router.routeByPatterns(loc("s3://src-bucket/f34b.logger.activity.schema.activity_log/2017/12/04/20171204_0221_0_23bcc31f-b9fd-406e-abf0-09a7cea072ca.gz"));
        assertNotNull(result);
        assertEquals(loc("s3://dest-bucket/f34b.logger.activity.schema.activity_log/2017/12/04/20171204_0221_0_23bcc31f-b9fd-406e-abf0-09a7cea072ca.gz"), result.getDestLocator());
        assertEquals("schema.activity_log", result.getStreamName());

        val table = tableRepos.findTable("schema", "activity_log");
        assertNotNull(table);
        assertEquals("schema", table.getSchemaName());
        assertEquals("activity_log", table.getTableName());

        val stream = streamRepos.findStream("schema.activity_log");
        assertNotNull(stream);
        assertEquals("schema.activity_log", stream.getStreamName());
        assertEquals(table, stream.getTable());

        val bundle = bundleRepos.findStreamBundle(stream, "src-bucket", "f34b.logger.activity.schema.activity_log");
        assertNotNull(bundle);
        assertEquals("src-bucket", bundle.getBucket());
        assertEquals("f34b.logger.activity.schema.activity_log", bundle.getPrefix());
        assertEquals(stream, bundle.getStream());
    }

    @Test
    public void routeByPatterns_find() throws Exception {
        val table0 = entityManager.persist(new TargetTable("schema", "activity_log", "schema.activity_log", "dest-bucket", "f34b.logger.activity.schema.activity_log"));
        val stream0 = entityManager.persist(new PacketStream("schema.activity_log", table0));
        val bundle0 = entityManager.persist(new StreamBundle(stream0, "src-bucket", "f34b.logger.activity.schema.activity_log"));

        val router = newRouter(entry("s3://src-bucket/(\\w{4}\\.(?:\\w+\\.\\w+\\.)?(\\w+\\.\\w+))/(\\d{4}/\\d{2}/\\d{2})/(.*\\.gz)", "$2", "$1", "dest-bucket", "$1", "$3", "$4"));
        router.check();
        val result = router.routeByPatterns(loc("s3://src-bucket/f34b.logger.activity.schema.activity_log/2017/12/04/20171204_0221_0_23bcc31f-b9fd-406e-abf0-09a7cea072ca.gz"));
        assertNotNull(result);
        assertEquals(loc("s3://dest-bucket/f34b.logger.activity.schema.activity_log/2017/12/04/20171204_0221_0_23bcc31f-b9fd-406e-abf0-09a7cea072ca.gz"), result.getDestLocator());
        assertEquals("schema.activity_log", result.getStreamName());

        val table = tableRepos.findTable("schema", "activity_log");
        assertNotNull(table);
        assertEquals(table0, table);
        assertEquals("schema", table.getSchemaName());
        assertEquals("activity_log", table.getTableName());

        val stream = streamRepos.findStream("schema.activity_log");
        assertNotNull(stream);
        assertEquals(stream0, stream);
        assertEquals("schema.activity_log", stream.getStreamName());
        assertEquals(table, stream.getTable());

        val bundle = bundleRepos.findStreamBundle(stream, "src-bucket", "f34b.logger.activity.schema.activity_log");
        assertNotNull(bundle);
        assertEquals(bundle0, bundle);
        assertEquals("src-bucket", bundle.getBucket());
        assertEquals("f34b.logger.activity.schema.activity_log", bundle.getPrefix());
        assertEquals(stream, bundle.getStream());
    }

    /* FIXME: temporary off: We should not try to route local file
    @Test
    public void route_localfile() throws Exception {
        val router = newRouter(entry("file:/(?:.+/)?src-bucket/src-prefix/(schema\\.table)/(.*\\.gz)", "$1", "src-prefix", "dest-bucket", "dest-prefix/$1", "", "$2"));
        router.check();
        val result = router.routeWithoutDB(loc("file:/path/to/src-bucket/src-prefix/schema.table/datafile.json.gz"));
        assertEquals(loc("s3://dest-bucket/dest-prefix/schema.table/datafile.json.gz"), result.getDestLocator());
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

    @Test
    public void route() throws Exception {
        val t = new TargetTable("schema", "table", "schema.table", "dest-bucket-2", "dest-prefix-2");
        entityManager.persist(t);
        val s = new PacketStream("schema.table", t);
        s.initialized = true;
        s.columnInitialized = true;
        entityManager.persist(s);
        val stream = streamRepos.findStream("schema.table");
        entityManager.persist(new StreamBundle(stream, "src-bucket", "0000.schema.table_2"));
        val bundle = bundleRepos.findStreamBundle("src-bucket-2", "src-prefix-2");

        val router = newRouter(entry("s3://src-bucket/(0000.(schema\\.table))/(2017/11/28)/(.*\\.gz)", "$2", "$1", "dest-bucket", "dest/$1", "$2", "$3"));
        router.check();

        val result = router.route(loc("s3://src-bucket/0000.schema.table_2/2017/11/28/datafile.json.gz"));
        assertNotNull(result);
        assertEquals("schema.table", result.getStreamName());
        assertEquals(loc("s3://dest-bucket-2/dest-prefix-2/2017/11/28/datafile.json.gz"), result.getDestLocator());

        assertNull(router.route(loc("s3://src-bucket-UNKNOWN/src-prefix-UNKNOWN/schema.table/datafile.json.gz")));
        assertNull(router.route(loc("s3://src-bucket-2/datafile.json.gz")));
    }

    @Test
    public void route_blackhole() throws Exception {
        val router = newRouter(blackholeEntry("s3://src-bucket/tmp/.*"));
        router.check();

        val result = router.route(loc("s3://src-bucket/tmp/this_file_is_ignored.gz"));
        assertNotNull(result);
        assertEquals(true, result.isBlackhole());
    }

    @Test
    public void isBadStreamName() throws Exception {
        assertFalse(PacketRouter.isBadStreamName("schema.table"));
        assertFalse(PacketRouter.isBadStreamName("schema_name.table_name"));
        assertFalse(PacketRouter.isBadStreamName("schema_name.table2"));
        assertFalse(PacketRouter.isBadStreamName("schema_name.table_name2"));
        assertFalse(PacketRouter.isBadStreamName("a_bit_long_schema_name.really_very_long_log_table_name"));

        assertTrue(PacketRouter.isBadStreamName(""));
        assertTrue(PacketRouter.isBadStreamName("stream"));
        assertTrue(PacketRouter.isBadStreamName("stream_name"));
        assertTrue(PacketRouter.isBadStreamName("schema.123456"));
        assertTrue(PacketRouter.isBadStreamName("schema.table-name"));
        assertTrue(PacketRouter.isBadStreamName("schema.stream[new]"));
        assertTrue(PacketRouter.isBadStreamName("42.log_table_name"));
    }
}
