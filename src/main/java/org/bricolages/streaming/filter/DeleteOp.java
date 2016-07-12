package org.bricolages.streaming.filter;
import lombok.*;

class DeleteOp extends SingleColumnOp {
    static final void register() {
        Op.registerOperator("delete", (def) ->
            new DeleteOp(def)
        );
    }

    DeleteOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        return null;
    }
}
