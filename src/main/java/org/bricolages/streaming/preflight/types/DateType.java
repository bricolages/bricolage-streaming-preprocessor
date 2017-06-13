package org.bricolages.streaming.preflight.types;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("date")
@MultilineDescription("Date")
@NoArgsConstructor
public class DateType extends PrimitiveType {
    @Getter private final String type = "date";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list;
    }

    // This is necessary to accept empty value
    @JsonCreator public DateType(String nil) { /* noop */ }
}
