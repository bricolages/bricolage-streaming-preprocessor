package org.bricolages.streaming.preflight.domains;
import org.bricolages.streaming.preflight.definition.DomainParameters;
import org.bricolages.streaming.preflight.definition.OperatorDefinitionEntry;
import org.bricolages.streaming.stream.StreamColumn;
import java.util.List;
import java.util.ArrayList;
import lombok.*;

public abstract class PrimitiveDomain implements DomainParameters {
    public final String getName() {
        return null; // primitive time does not specify column name
    }

    public final String getOriginalName() {
        return null;
    }

    public List<OperatorDefinitionEntry> getOperatorDefinitionEntries() {
        return new ArrayList<OperatorDefinitionEntry>();
    }

    // default implementation
    public StreamColumn.Params getStreamColumnParams() {
        val params = new StreamColumn.Params();
        params.type = getType();
        return params;
    }
}
