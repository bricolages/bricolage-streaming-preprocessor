package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("date")
@MultilineDescription("Date")
@NoArgsConstructor
public class DateDomain extends PrimitiveDomain {
    @Getter private final String type = "date";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    @Getter
    @MultilineDescription("Source timezone, given by the string like '+00:00'")
    private String sourceOffset;

    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String zoneOffset;

    // This is necessary to accept empty value
    @JsonCreator public DateDomain(String nil) { /* noop */ }

    public StreamColumn.Params getStreamColumnParams() {
        val params = super.getStreamColumnParams();
        params.sourceOffset = sourceOffset;
        params.zoneOffset = zoneOffset;
        return params;
    }
}
