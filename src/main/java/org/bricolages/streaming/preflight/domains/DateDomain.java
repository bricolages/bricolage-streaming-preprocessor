package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.definition.ColumnEncoding;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("date")
@MultilineDescription("Date")
@NoArgsConstructor
public class DateDomain extends PrimitiveDomain {
    @Getter private final String type = "date";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list;
    }

    // This is necessary to accept empty value
    @JsonCreator public DateDomain(String nil) { /* noop */ }
}
