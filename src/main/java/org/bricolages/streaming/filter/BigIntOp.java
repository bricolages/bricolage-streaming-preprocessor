package org.bricolages.streaming.filter;
import lombok.*;

class BigIntOp extends SingleColumnOp {
    static {
        Op.registerOperator("bigint", (def) ->
            new BigIntOp(def)
        );
    }

    BigIntOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        long i = getInteger(value);
        return Long.valueOf(i);
    }
}
