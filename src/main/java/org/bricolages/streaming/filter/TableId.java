package org.bricolages.streaming.filter;
import lombok.*;

@EqualsAndHashCode
public class TableId {
    static public TableId parse(String spec) {
        // FIXME
        return new TableId(spec);
    }

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
