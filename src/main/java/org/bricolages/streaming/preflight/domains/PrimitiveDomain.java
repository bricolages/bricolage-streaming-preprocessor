package org.bricolages.streaming.preflight.domains;

import org.bricolages.streaming.preflight.definition.DomainParameters;

public abstract class PrimitiveDomain implements DomainParameters {
    public final String getName() {
        return null; // primitive time does not specify column name
    }

    public final String getOriginalName() {
        return null;
    }
}
