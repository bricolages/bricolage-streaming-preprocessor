package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.TimeZoneOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("date")
public class DateDomain implements ColumnParametersEntry {
    @Getter
    @JsonProperty(required = true)
    private String sourceOffset;
    @Getter
    @JsonProperty(required = true)
    private String targetOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val tzParams = new TimeZoneOp.Parameters();
        tzParams.setSourceOffset(sourceOffset);
        tzParams.setTargetOffset(targetOffset);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("timezone", columnName, tzParams));
        return list;
    }
}
