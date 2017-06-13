package org.bricolages.streaming.preflight.types;
import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.filter.UnixTimeOp;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import org.bricolages.streaming.ConfigError;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("unixtime")
@MultilineDescription("Timestamp converted from unix time")
@NoArgsConstructor
public class UnixtimeType extends PrimitiveType {
    @Getter
    @MultilineDescription("Target timezone, given by the string like '+09:00'")
    private String zoneOffset;

    @Getter private final String type = "timestamp";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        if (zoneOffset == null) {
            throw new ConfigError("missing parameter: zoneOffset");
        }
        val utParams = new UnixTimeOp.Parameters();
        utParams.setZoneOffset(zoneOffset);
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("unixtime", columnName, utParams));
        return list;
    }

    // This is necessary to accept empty value
    @JsonCreator public UnixtimeType(String nil) { /* noop */ }
}
