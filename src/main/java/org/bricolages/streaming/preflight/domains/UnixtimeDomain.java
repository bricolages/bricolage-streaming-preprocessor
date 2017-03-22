package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.UnixTimeOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("unixtime")
@JsonClassDescription("Timestamp converted from unix time")
public class UnixtimeDomain implements ColumnParametersEntry {
    @Getter
    @JsonProperty(required = true)
    @JsonPropertyDescription("Target timezone, given by the string like '+09:00'")
    private String zoneOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val utParams = new UnixTimeOp.Parameters();
        utParams.setZoneOffset(zoneOffset);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("unixtime", columnName, utParams));
        return list;
    }
}
