package org.bricolages.streaming.s3;
import org.bricolages.streaming.ConfigError;
import org.bricolages.streaming.SourceLocator;

import java.util.Objects;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.regex.PatternSyntaxException;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
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

    public Result map(String src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src);
            if (m.matches()) {
                return new Result(
                    safeSubst(ent.streamName, m),
                    ent.destBucket,
                    safeSubst(ent.streamPrefix, m),
                    safeSubst(ent.objectPrefix, m),
                    safeSubst(ent.objectName, m)
                );
            }
        }
        // FIXME: error??
        log.error("unknown S3 object URL: {}", src);
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
        @Setter public String destBucket;
        @Setter public String streamPrefix;
        @Setter public String objectPrefix;
        @Setter public String objectName;

        public Entry(String srcUrlPattern, String streamName, String destBucket, String streamPrefix, String objectPrefix, String objectName) {
            this.srcUrlPattern = srcUrlPattern;
            this.streamName = streamName;
            this.destBucket = destBucket;
            this.streamPrefix = streamPrefix;
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

        @Getter private final String destBucket;
        @Getter private final String streamPrefix;
        @Getter private final String objectPrefix;
        @Getter private final String objectName;

        public S3ObjectLocation getDestLocation() {
            return new S3ObjectLocation(destBucket, Paths.get(streamPrefix, objectPrefix, objectName).toString());
        }
    }
}
