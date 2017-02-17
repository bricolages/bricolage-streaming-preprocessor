package org.bricolages.streaming.s3;
import org.bricolages.streaming.ConfigError;
import java.util.Objects;
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
                throw new ConfigError("source pattern syntax error: " + ent.src);
            }
        }
    }

    public Result map(S3ObjectLocation src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.urlString());
            if (m.matches()) {
                try {
                    val dest = S3ObjectLocation.forUrl(safeSubst(ent.dest, m, src));
                    val streamName = safeSubst(ent.table, m, src);
                    return new Result(dest, streamName);
                }
                catch (S3UrlParseException ex) {
                    throw new ConfigError("S3 URL parse error: " + ex.getMessage());
                }
            }
        }
        // FIXME: error??
        log.error("unknown S3 object URL: {}", src);
        return null;
    }

    String safeSubst(String template, Matcher m, S3ObjectLocation src) {
        String result;
        try {
            result = m.replaceFirst(template);
        }
        catch (IndexOutOfBoundsException ex) {
            throw new ConfigError("bad replacement: " + template + ", src=" + src);
        }
        if (Objects.equals(src.toString(), result)) {
            throw new ConfigError("could not map object url: src=" + src + ", template=" + template);
        }
        return result;
    }

    @NoArgsConstructor
    public static final class Entry {
        @Getter @Setter String src;
        @Getter @Setter String dest;
        @Getter @Setter String table;

        Entry(String src, String dest, String table) {
            this.src = src;
            this.dest = dest;
            this.table = table;
        }

        Pattern pat = null;

        Pattern sourcePattern() {
            if (pat != null) return pat;
            pat = Pattern.compile("^" + src + "$");
            return pat;
        }
    }

    @RequiredArgsConstructor
    public static final class Result {
        @Getter final S3ObjectLocation destLocation;
        @Getter final String streamName;
    }
}
