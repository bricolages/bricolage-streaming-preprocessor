package org.bricolages.streaming.preflight.domains;

import java.util.ArrayList;
import java.util.List;
import org.bricolages.streaming.preflight.ColumnEncoding;
import org.bricolages.streaming.preflight.ColumnParametersEntry;
import org.bricolages.streaming.preflight.OperatorDefinitionEntry;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("string")
public class StringDomain implements ColumnParametersEntry {
    @Getter
    @JsonProperty(required = true)
    private int bytes;

    public String getType() {
        return String.format("varchar(%d)", bytes);
    }
    @Getter private final ColumnEncoding encoding = ColumnEncoding.LZO;

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        val list = new ArrayList<OperatorDefinitionEntry>();
        return list; // empty list
    }

    @JsonCreator
    public StringDomain(String bytes) {
        this.bytes = Integer.valueOf(bytes);
    }
}
