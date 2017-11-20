package org.bricolages.streaming.s3;
import org.bricolages.streaming.ApplicationError;
import org.bricolages.streaming.ConfigError;
import org.bricolages.streaming.SourceLocator;
import org.bricolages.streaming.DataStream;
import org.bricolages.streaming.DataStreamRepository;
import org.bricolages.streaming.StreamBundle;
import org.bricolages.streaming.StreamBundleRepository;
import org.springframework.beans.factory.annotation.Autowired;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@RequiredArgsConstructor
@Slf4j
public class ObjectMapper {
    final List<Entry> entries;

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

    public Result map(S3ObjectLocation loc) throws ConfigError {
        Result r1 = mapByPrefix(loc.bucket(), loc.key());
        if (r1 != null) return r1;
        Result r2 = mapByPatterns(loc.urlString());
        if (r2 != null) return r2;
        logUnknownS3Object(loc.urlString());
        return null;
    }

    @Autowired
    DataStreamRepository streamRepos;

    @Autowired
    StreamBundleRepository streamBundleRepos;

    Result mapByPrefix(String bucket, String key) {
        val components = key.split("/");
        if (components.length < 3) {
            logUnknownS3Object("s3://" + bucket + "/" + key);
            return null;
        }
        val prefix = components[0];
        val objName = components[components.length - 1];
        String[] prefixes = Arrays.copyOfRange(components, 1, components.length - 1);
        val objPrefix = String.join("/", prefixes);

        val bundle = streamBundleRepos.findStreamBundle(bucket, prefix);
        if (bundle == null) return null;
        val stream = bundle.getStream();
        if (stream == null) throw new ApplicationError("FATAL: could not get stream for stream_bundle: stream_bundle_id=" + bundle.getId());
        return new Result(
            stream.getStreamName(),
            bundle.getPrefix(),
            bundle.getDestBucket(),
            bundle.getDestPrefix(),
            objPrefix,
            objName
        );
    }

    public void logUnknownS3Object(String srcUrl) {
        // FIXME: error??
        log.error("unknown S3 object URL: {}", srcUrl);
    }

    public Result mapByPatterns(String srcUrl) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(srcUrl);
            if (m.matches()) {
                return new Result(
                    safeSubst(ent.streamName, m),
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

    @NoArgsConstructor
    public static final class Entry {
        @Setter public String srcUrlPattern;
        @Setter public String streamName;
        @Setter public String streamPrefix;
        @Setter public String destBucket;
        @Setter public String destPrefix;
        @Setter public String objectPrefix;
        @Setter public String objectName;

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
    }

    @RequiredArgsConstructor
    public static final class Result {
        @Getter private final String streamName;
        @Getter private final String streamPrefix;
        @Getter private final String destBucket;
        @Getter private final String destPrefix;
        @Getter private final String objectPrefix;
        @Getter private final String objectName;

        public S3ObjectLocation getDestLocation() {
            return new S3ObjectLocation(destBucket, Paths.get(destPrefix, objectPrefix, objectName).toString());
        }
    }
}
