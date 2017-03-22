package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("float")
@JsonClassDescription("Floating point number")
public class FloatDomain implements ColumnParametersEntry {
    @Getter private final String type = "float";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("float", columnName, new HashMap<>()));
        return list;
    }

    // This is necessary to accept null value
    @JsonCreator public FloatDomain(String nil) { /* noop */ }
}
