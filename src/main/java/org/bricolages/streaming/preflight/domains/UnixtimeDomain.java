package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.exception.*;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("unixtime")
@MultilineDescription("Timestamp converted from unix time")
@NoArgsConstructor
public class UnixtimeDomain extends PrimitiveDomain {
    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String zoneOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    // This is necessary to accept empty value
    @JsonCreator public UnixtimeDomain(String nil) { /* noop */ }

    public StreamColumn.Params getStreamColumnParams() {
        val params = super.getStreamColumnParams();
        params.type = "timestamp";
        params.sourceOffset = "+00:00";
        params.zoneOffset = zoneOffset;
        return params;
    }
}
