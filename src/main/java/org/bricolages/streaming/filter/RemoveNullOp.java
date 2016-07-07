package org.bricolages.streaming.filter;
import lombok.*;

class RemoveNullOp extends Op {
    static final void register() {
        Op.registerOperator("removenull", (def) ->
            new RemoveNullOp(def)
        );
    }

    RemoveNullOp(OperatorDefinition def) {
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
