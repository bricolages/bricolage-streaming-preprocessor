package org.bricolages.streaming.preflight;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.*;

public class ColumnDefinitionEntry {
    @Getter private String columName;
    @Getter private ColumnParametersEntry params;

    @JsonCreator
    public ColumnDefinitionEntry(Map.Entry<String, ColumnParametersEntry> kv) {
        this.columName = kv.getKey();
        this.params = kv.getValue();
    }
}
