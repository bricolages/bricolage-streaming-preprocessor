package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import lombok.*;

public class RenameOp extends Op {
    static final void register(OpBuilder builder) {
        builder.registerOperator("rename", (def) ->
            new RenameOp(def, def.mapParameters(Parameters.class))
        );
    }

    @Getter
    @Setter
    public static class Parameters {
        String to;
    }

    final String to;

    RenameOp(OperatorDefinition def, Parameters params) {
        this(def, params.to);
    }

    RenameOp(OperatorDefinition def, String to) {
        super(def);
        this.to = to;
    }

    @Override
    public Record apply(Record record) {
        String from = getColumnName();
        Object value = record.get(from);
        record.remove(from);
        if (value != null) {
            record.put(to, value);
        }
        return (record.isEmpty() ? null : record);
    }
}
