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

@JsonTypeName("date")
@MultilineDescription("Date time")
@NoArgsConstructor
public class DateDomain implements ColumnParametersEntry {
    @Getter
    @MultilineDescription("Expected source data timezone, given by the string like '+00:00'")
    private String sourceOffset;
    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String targetOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val tzParams = new TimeZoneOp.Parameters();
        tzParams.setSourceOffset(sourceOffset);
        tzParams.setTargetOffset(targetOffset);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("timezone", columnName, tzParams));
        return list;
    }

    public void applyDefault(DomainDefaultValues defaultValues) {
        val defaultValue = defaultValues.getDate();
        if (defaultValue == null) { return; }
        this.sourceOffset = this.sourceOffset == null ? defaultValue.sourceOffset : this.sourceOffset;
        this.targetOffset = this.targetOffset == null ? defaultValue.targetOffset : this.targetOffset;
    }

    // This is necessary to accept empty value
    @JsonCreator public DateDomain(String nil) { /* noop */ }
}
