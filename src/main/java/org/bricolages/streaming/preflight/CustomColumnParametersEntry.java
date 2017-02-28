package org.bricolages.streaming.preflight;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import lombok.*;

@JsonTypeName("custom")
class CustomColumnParametersEntry implements ColumnParametersEntry {
    @JsonProperty(required = true)
    @Getter private String type;
    @JsonProperty(required = true)
    @Getter private ColumnEncoding encoding = ColumnEncoding.RAW;
    @JsonProperty(required = true)
    private List<OperatorDefinitionEntry> filter = new ArrayList<>();
    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries(String columnName) {
        return filter;
    }
}