package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.RenameOp;
import org.bricolages.streaming.filter.TimeZoneOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("log_time")
@MultilineDescription({
    "Timestamp indicating when log was recorded",
    "Usually this column becomes sortkey.",
})
@NoArgsConstructor
public class LogTimeDomain implements ColumnParametersEntry {
    @Getter
    @MultilineDescription("Expected source data timezone, given by the string like '+00:00'")
    private String sourceOffset;
    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String targetOffset;
    @Getter
    @MultilineDescription("Column name which is renamed from")
    private String sourceColumn;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val tzParams = new TimeZoneOp.Parameters();
        tzParams.setSourceOffset(sourceOffset);
        tzParams.setTargetOffset(targetOffset);
        tzParams.setTruncate(false);
        val renameParams = new RenameOp.Parameters();
        renameParams.setTo(columnName);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("timezone", sourceColumn, tzParams));
        list.add(new OperatorDefinitionEntry("rename", sourceColumn, renameParams));
        return list;
    }

    public void applyDefault(DomainDefaultValues defaultValues) {
        val defaultValue = defaultValues.getLogTime();
        if (defaultValue == null) { return; }
        this.sourceOffset = this.sourceOffset == null ? defaultValue.sourceOffset : this.sourceOffset;
        this.targetOffset = this.targetOffset == null ? defaultValue.targetOffset : this.targetOffset;
        this.sourceColumn = this.sourceColumn == null ? defaultValue.sourceColumn : this.sourceColumn;
    }

    // This is necessary to accept empty value
    @JsonCreator public LogTimeDomain(String nil) { /* noop */ }
}
