package org.bricolages.streaming.preflight.types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("integer")
@MultilineDescription("32bit signed integral number")
public class IntegerType extends PrimitiveType {
    @Getter private final String type = "integer";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("int", new HashMap<>()));
        return list;
    }

    // This is necessary to accept null value
    @JsonCreator public IntegerType(String nil) { /* noop */ }
}
