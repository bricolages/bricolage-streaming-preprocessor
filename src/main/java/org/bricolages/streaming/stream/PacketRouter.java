package org.bricolages.streaming.stream;
import org.bricolages.streaming.object.S3ObjectLocator;
import org.bricolages.streaming.table.*;
import org.bricolages.streaming.exception.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PacketRouter {
    @NoArgsConstructor
    public static final class Entry {
        @Setter public String srcUrlPattern;
        @Setter public String streamName;
        @Setter public String streamPrefix;
        @Setter public String destBucket;
        @Setter public String destPrefix;
        @Setter public String objectPrefix;
        @Setter public String objectName;
        @Setter public Boolean blackhole = false;

        public Entry(String srcUrlPattern, String streamName, String streamPrefix, String destBucket, String destPrefix, String objectPrefix, String objectName) {
            this.srcUrlPattern = srcUrlPattern;
            this.streamName = streamName;
            this.streamPrefix = streamPrefix;
            this.destBucket = destBucket;
            this.destPrefix = destPrefix;
            this.objectPrefix = objectPrefix;
            this.objectName = objectName;
        }

        Pattern pat = null;

        Pattern sourcePattern() {
            if (pat != null) return pat;
            pat = Pattern.compile("^" + srcUrlPattern + "$");
            return pat;
        }

        public String description() {
            if (blackhole) {
                return srcUrlPattern + " -> (blackhole)";
            }
            else {
                return srcUrlPattern + " -> s3://" + destBucket + "/" + destPrefix + "/" + objectPrefix + "/" + objectName;
            }
        }
    }

    @Getter final List<Entry> entries;

    @Autowired
    PacketFilterFactory filterFactory;

    public PacketRouter(List<Entry> entries) {
        this.entries = entries;
        log.info("Routing patterns registered: {} entries", entries.size());
    }

    void check() throws ConfigError {
        for (Entry ent : entries) {
            try {
                ent.sourcePattern();
            }
            catch (PatternSyntaxException ex) {
                throw new ConfigError("source pattern syntax error: " + ent.srcUrlPattern);
            }
        }
    }

    public Route route(S3ObjectLocator src) throws ConfigError {
        val r1 = routeBySavedRoutes(src);
        if (r1 != null) return r1;
        val r2 = routeByPatterns(src);
        if (r2 != null) return r2;

        logUnknownS3Object(src);
        return null;
    }

    @Autowired
    TargetTableRepository tableRepos;

    @Autowired
    PacketStreamRepository streamRepos;

    @Autowired
    StreamBundleRepository bundleRepos;

    /**
     * Expects URL like: s3://src-bucket/prefix1/prefix2/prefix3/.../YYYY/MM/DD/objectName.gz
     * streamPrefix: "prefix1/prefix2/prefix3/..."
     * objectPrefix: "YYYY/MM/DD"
     * objectName: "objectName.gz"
     */
    Route routeBySavedRoutes(S3ObjectLocator src) {
        val components = src.key().split("/");
        if (components.length < 5) {
            log.info("could not apply routeBySavedRoutes: {}", src);
            return null;
        }
        String[] prefixComponents = Arrays.copyOfRange(components, 0, components.length - 4);
        val prefix = String.join("/", prefixComponents);
        String[] objPrefixComponents = Arrays.copyOfRange(components, components.length - 4, components.length - 1);
        val objPrefix = String.join("/", objPrefixComponents);
        val objName = components[components.length - 1];
        log.debug("parsed url: prefix={}, objPrefix={}, objName={}", prefix, objPrefix, objName);

        val bundle = bundleRepos.findStreamBundle(src.bucket(), prefix);
        if (bundle == null) return null;
        val stream = bundle.getStream();
        if (stream == null) throw new ApplicationError("FATAL: could not get stream for stream_bundle: stream_bundle_id=" + bundle.getId());

        return new Route(filterFactory, stream, bundle, objPrefix, objName);
    }

    Route routeByPatterns(S3ObjectLocator src) throws ConfigError {
        val components = matchRoutes(src);
        if (components == null) return null;
        if (components.isEmpty()) return Route.makeBlackhole();

        PacketStream stream = streamRepos.findStream(components.streamName);
        if (stream == null) {
            // If a stream does not exist, its corresponding table does not exist, too.
            // In other words, a stream and a table is coupled tightly.
            val name = TableSpec.parse(components.streamName);
            val table = tableRepos.findOrCreate(name.schema, name.table, components.streamName, components.destBucket, components.destPrefix, log);
            stream = streamRepos.createForce(components.streamName, table, log);
        }
        val bundle = bundleRepos.findOrCreate(stream, components.srcBucket, components.srcPrefix, log);
        return new Route(filterFactory, stream, bundle, components.objectPrefix, components.objectName);
    }

    // For preflight
    public Route routeWithoutDB(S3ObjectLocator src) throws ConfigError {
        val components = matchRoutes(src);
        if (components == null) return null;
        val names = components.streamName.split("\\.");
        val table = new TargetTable(names[0], names[1], components.streamName, components.destBucket, components.destPrefix);
        val stream = new PacketStream(components.streamName, table);
        val bundle = new StreamBundle(stream, components.srcBucket, components.srcPrefix);
        return new Route(filterFactory, stream, bundle, components.objectPrefix, components.objectName);
    }

    RouteComponents matchRoutes(S3ObjectLocator src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.toString());
            if (m.matches()) {
                if (ent.blackhole) {
                    return RouteComponents.makeEmpty();
                }
                return new RouteComponents(
                    safeSubst(ent.streamName, m),
                    src.bucket(),
                    safeSubst(ent.streamPrefix, m),
                    ent.destBucket,
                    safeSubst(ent.destPrefix, m),
                    safeSubst(ent.objectPrefix, m),
                    safeSubst(ent.objectName, m)
                );
            }
        }
        return null;
    }

    String safeSubst(String template, Matcher m) throws ConfigError {
        try {
            return m.replaceFirst(template);
        }
        catch (IndexOutOfBoundsException ex) {
            throw new ConfigError("bad replacement: " + template);
        }
    }

    @RequiredArgsConstructor
    static final class RouteComponents {
        final String streamName;
        final String srcBucket;
        final String srcPrefix;
        final String destBucket;
        final String destPrefix;
        final String objectPrefix;
        final String objectName;

        static RouteComponents makeEmpty() {
            return new RouteComponents(null, null, null, null, null, null, null);
        }

        boolean isEmpty() {
            return this.streamName == null;
        }
    }

    @AllArgsConstructor
    static final class TableSpec {
        public final String schema;
        public final String table;

        static public TableSpec parse(String spec) {
            val names = spec.split("\\.");
            return new TableSpec(names[0], names[1]);
        }
    }

    public void logUnknownS3Object(S3ObjectLocator loc) {
        log.warn("unknown S3 object URL: {}", loc);
    }
}
