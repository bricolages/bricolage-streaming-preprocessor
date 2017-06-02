package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import org.bricolages.streaming.preflight.ReferenceGenerator.MultilineDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("integer")
@MultilineDescription("32bit signed integral number")
public class IntegerDomain implements ColumnParametersEntry {
    @Getter private final String type = "integer";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.ZSTD;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("int", columnName, new HashMap<>()));
        return list;
    }

    // This is necessary to accept null value
    @JsonCreator public IntegerDomain(String nil) { /* noop */ }
}
