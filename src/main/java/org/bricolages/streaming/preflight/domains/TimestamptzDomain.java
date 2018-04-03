package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.exception.ConfigError;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("timestamptz")
@MultilineDescription("Timestamp with time zone")
@NoArgsConstructor
public class TimestamptzDomain extends PrimitiveDomain {
    @Getter
    @MultilineDescription("Source timezone, given by the string like '+00:00'")
    private String sourceOffset;

    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String targetOffset;

    @Getter private final String type = "timestamptz";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    // This is necessary to accept empty value
    @JsonCreator public TimestamptzDomain(String nil) { /* noop */ }

    public StreamColumn.Params getStreamColumnParams() {
        val params = super.getStreamColumnParams();
        params.sourceOffset = this.sourceOffset;
        params.zoneOffset = this.targetOffset;
        return params;
    }
}
