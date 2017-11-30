package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.filter.UnixTimeOp;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.exception.*;
import java.util.ArrayList;
import java.util.List;
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

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        if (zoneOffset == null) {
            throw new ConfigError("missing parameter: zoneOffset");
        }
        val utParams = new UnixTimeOp.Parameters();
        utParams.setZoneOffset(zoneOffset);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("unixtime", utParams));
        return list;
    }

    // This is necessary to accept empty value
    @JsonCreator public UnixtimeDomain(String nil) { /* noop */ }
}
