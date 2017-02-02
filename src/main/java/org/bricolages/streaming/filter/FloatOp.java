package org.bricolages.streaming.filter;
import lombok.*;

class FloatOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("float", (def) ->
            new FloatOp(def)
        );
    }

    FloatOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object rawValue, Record record) throws FilterException {
        if (rawValue == null) return null;
        float value = getFloat(rawValue);
        if (Float.MIN_VALUE <= value && value <= Float.MAX_VALUE) {
            return Float.valueOf(value);
        }
        else {
            return null;
        }
    }
}
