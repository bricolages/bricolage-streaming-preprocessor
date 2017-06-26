package org.bricolages.streaming.preflight.definition;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.*;

@RequiredArgsConstructor
public class DomainParametersEntry implements DomainParameters {
    @JsonProperty("<<") // TRICK: override merge `<<` operator
    protected final DomainParameters parent;
    protected final String type;
    protected final ColumnEncoding encoding;
    protected final List<OperatorDefinitionEntry> filter;
    protected final List<OperatorDefinitionEntry> prependFilter;
    protected final List<OperatorDefinitionEntry> appendFilter;

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
