package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;
import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("boolean")
@JsonClassDescription("Boolean")
public class BooleanDomain implements ColumnParametersEntry {
    @Getter private final String type = "boolean";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.RAW;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list; // empty list
    }

    // This is necessary to accept null value
    @JsonCreator public BooleanDomain(String nil) { /* noop */ }
}
