package org.bricolages.streaming.filter;
import lombok.*;

public class FloatOp extends SingleColumnOp {
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
        float value = (float)Cleanse.getFloat(rawValue);
        // getFloat returns Inf/-Inf for too big/small value
        if (Float.isInfinite(value) || Float.isNaN(value)) return null;
        return Float.valueOf(value);
    }
}
