package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.RenameOp;
import org.bricolages.streaming.filter.TimeZoneOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("log_time")
public class LogTimeDomain implements ColumnParametersEntry {
    @Getter
    @JsonProperty(required = true)
    private String sourceOffset;
    @Getter
    @JsonProperty(required = true)
    private String targetOffset;
    @Getter
    @JsonProperty(required = true)
    private String sourceColumn;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

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
}
