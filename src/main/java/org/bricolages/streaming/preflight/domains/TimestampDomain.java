package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.filter.TimeZoneOp;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.exception.ConfigError;
import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("timestamp")
@MultilineDescription("Timestamp with zone adjust")
@NoArgsConstructor
public class TimestampDomain extends PrimitiveDomain {
    @Getter
    @MultilineDescription("Source timezone, given by the string like '+00:00'")
    private String sourceOffset;

    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String targetOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        if (sourceOffset == null) {
            throw new ConfigError("missing parameter: sourceOffset");
        }
        if (targetOffset == null) {
            throw new ConfigError("missing parameter: targetOffset");
        }
        val params = new TimeZoneOp.Parameters();
        params.setSourceOffset(sourceOffset);
        params.setTargetOffset(targetOffset);
        val ops = new ArrayList<OperatorDefinitionEntry>();
        ops.add(new OperatorDefinitionEntry("timezone", params));
        return ops;
    }

    // This is necessary to accept empty value
    @JsonCreator public TimestampDomain(String nil) { /* noop */ }
}
