package org.bricolages.streaming.preflight.types;

import org.bricolages.streaming.preflight.ColumnParametersEntry;

public abstract class PrimitiveType implements ColumnParametersEntry {
    public final String getName() {
        return null; // primitive time does not specify column name
    }

    public final String getOriginalName() {
        return null;
    }
}
