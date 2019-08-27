package org.bricolages.streaming.preflight.definition;

import org.bricolages.streaming.stream.StreamColumn;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.val;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@NoArgsConstructor
public class DomainParametersEntry implements DomainParameters {
    @JsonProperty("<<") // TRICK: override merge `<<` operator
    protected DomainParameters parent;
    protected String type;
    protected ColumnEncoding encoding;
    @Getter protected List<OperatorDefinitionEntry> filter;
    @Getter protected List<OperatorDefinitionEntry> prependFilter;
    @Getter protected List<OperatorDefinitionEntry> appendFilter;

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

    public StreamColumn.Params getStreamColumnParams() {
        if (parent != null) {
            return parent.getStreamColumnParams();
        }
        else {
            return new StreamColumn.Params();
        }
    }
}
