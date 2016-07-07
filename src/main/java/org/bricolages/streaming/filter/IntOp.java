package org.bricolages.streaming.filter;
import lombok.*;

class IntOp extends SingleColumnOp {
    static final void register() {
        Op.registerOperator("int", (def) ->
            new IntOp(def)
        );
    }

    IntOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        long i = getInteger(value);
        if (Integer.MIN_VALUE <= i && i <= Integer.MAX_VALUE) {
            return Integer.valueOf((int)i);
        }
        else {
            return null;
        }
    }
}
