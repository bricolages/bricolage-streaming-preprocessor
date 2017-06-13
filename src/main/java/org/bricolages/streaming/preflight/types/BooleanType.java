package org.bricolages.streaming.preflight.types;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("boolean")
@MultilineDescription("Boolean")
public class BooleanType extends PrimitiveType {
    @Getter private final String type = "boolean";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list; // empty list
    }

    // This is necessary to accept null value
    @JsonCreator public BooleanType(String nil) { /* noop */ }
}
