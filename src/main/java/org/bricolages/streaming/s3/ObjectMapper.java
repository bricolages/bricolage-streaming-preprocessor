package org.bricolages.streaming.s3;
import org.bricolages.streaming.ConfigError;
import org.bricolages.streaming.filter.TableId;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import lombok.*;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Slf4j
public class ObjectMapper {
    final List<Entry> entries;

    void check() {
        for (Entry ent : entries) {
            ent.sourcePattern();
        }
    }

    public Result map(S3ObjectLocation src) throws ConfigError {
        for (Entry ent : entries) {
            Matcher m = ent.sourcePattern().matcher(src.urlString());
            if (m.matches()) {
                try {
                    val dest = S3ObjectLocation.forUrl(m.replaceFirst(ent.dest));
                    val table = new TableId("tmp.dest");   // FIXME: implement
                    return new Result(dest, table);
                }
                catch (S3UrlParseException ex) {
                    throw new ConfigError(ex);
                }
            }
        }
        // FIXME: error??
        log.error("unknown S3 object URL: {}", src);
        return null;
    }

    @NoArgsConstructor
    public static final class Entry {
        @Getter @Setter String src;
        @Getter @Setter String dest;

        Entry(String src, String dest) {
            this.src = src;
            this.dest = dest;
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
        @Getter final TableId tableId;
    }
}
