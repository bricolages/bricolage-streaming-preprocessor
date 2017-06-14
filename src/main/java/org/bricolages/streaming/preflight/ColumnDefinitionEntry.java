package org.bricolages.streaming.preflight;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@RequiredArgsConstructor
public class ColumnDefinitionEntry implements ColumnParametersEntry {
    @JsonProperty("<<") // TRICK: override merge `<<` operator
    private final ColumnParametersEntry parent;
    private final String name;
    private final String type;
    private final ColumnEncoding encoding;
    private final List<OperatorDefinitionEntry> filter;
    private final List<OperatorDefinitionEntry> prependFilter;
    private final List<OperatorDefinitionEntry> appendFilter;
    private final String originalName;

    public String getName() {
        if (name != null) { return name; }
        if (parent != null) { return parent.getName(); }
        return null;
    }

    public String getType() {
        if (type != null) { return type; }
        if (parent != null) { return parent.getType(); }
        return null;
    }

    public ColumnEncoding getEncoding() {
        if (encoding != null) { return encoding; }
        if (parent != null) { return parent.getEncoding(); }
        return null;
    }

    public String getOriginalName() {
        if (originalName != null) { return originalName; }
        if (parent != null) { return parent.getOriginalName(); }
        return null;
    }

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        val wholeFilter = new ArrayList<OperatorDefinitionEntry>();

        if (prependFilter != null) {
            wholeFilter.addAll(prependFilter);
        }
        if (filter != null) {
            wholeFilter.addAll(filter);
        } else {
            val parentFilter = parent.getOperatorDefinitionEntries();
            if (parentFilter != null) {
                wholeFilter.addAll(parentFilter);
            }
        }
        if (appendFilter != null) {
            wholeFilter.addAll(appendFilter);
        }

        return wholeFilter;
    }
}
