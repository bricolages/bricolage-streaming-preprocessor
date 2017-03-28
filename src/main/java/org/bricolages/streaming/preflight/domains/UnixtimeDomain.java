package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.UnixTimeOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("unixtime")
@MultilineDescription("Timestamp converted from unix time")
public class UnixtimeDomain implements ColumnParametersEntry {
    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
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

    public void applyDefault(DomainDefaultValues defaultValues) {
        val defaultValue = defaultValues.getUnixtime();
        if (defaultValue == null) { return; }
        this.zoneOffset = this.zoneOffset == null ? defaultValue.zoneOffset : this.zoneOffset;
    }
}
