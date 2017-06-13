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

@JsonTypeName("float")
@MultilineDescription("64bit floating point number")
public class FloatType extends PrimitiveType {
    @Getter private final String type = "float";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("float", columnName, new HashMap<>()));
        return list;
    }

    // This is necessary to accept null value
    @JsonCreator public FloatType(String nil) { /* noop */ }
}
