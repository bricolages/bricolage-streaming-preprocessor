package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.TimeZoneOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("timestamp")
@MultilineDescription("Timestamp with zone adjust")
@NoArgsConstructor
public class TimestampDomain implements ColumnParametersEntry {
    @Getter
    @MultilineDescription("Source timezone, given by the string like '+00:00'")
    private String sourceOffset;

    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String targetOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val params = new TimeZoneOp.Parameters();
        params.setSourceOffset(sourceOffset);
        params.setTargetOffset(targetOffset);
        val ops = new ArrayList<OperatorDefinitionEntry>();
        ops.add(new OperatorDefinitionEntry("timezone", columnName, params));
        return ops;
    }

    public void applyDefault(DomainDefaultValues defaultValues) {
        val defaultValue = defaultValues.getTimestamp();
        if (defaultValue == null) { return; }
        if (this.sourceOffset == null) {
            this.sourceOffset = defaultValue.sourceOffset;
        }
        if (this.targetOffset == null) {
            this.targetOffset = defaultValue.targetOffset;
        }
    }

    // This is necessary to accept empty value
    @JsonCreator public TimestampDomain(String nil) { /* noop */ }
}
