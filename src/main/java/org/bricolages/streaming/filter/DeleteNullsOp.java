package org.bricolages.streaming.filter;
import java.util.Map;
import lombok.*;

public class DeleteNullsOp extends Op {
    static final void register(OpBuilder builder) {
        builder.registerOperator("deletenulls", (def) ->
            new DeleteNullsOp(def)
        );
    }

    DeleteNullsOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Record apply(Record record) {
        val it = record.entries();
        while (it.hasNext()) {
            val ent = it.next();
            if (ent.getValue() == null) {
                it.remove();
            }
        }
        return (record.isEmpty() ? null : record);
    }
}
