package org.bricolages.streaming.filter;
import lombok.*;

@EqualsAndHashCode
public class TableId {
    final String spec;

    public TableId(String spec) {
        this.spec = spec;
    }

    // FIXME
    //String schemaName();
    //String tableName();

    public String toString() {
        return spec;
    }
}
