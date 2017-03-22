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

@JsonTypeName("bigint")
@JsonClassDescription("64bit signed integral number")
public class BigintDomain implements ColumnParametersEntry {
    @Getter private final String type = "bigint";
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        list.add(new OperatorDefinitionEntry("bigint", columnName, new HashMap<>()));
        return list;
    }

    // This is necessary to accept null value
    @JsonCreator public BigintDomain(String nil) { /* noop */ }
}
