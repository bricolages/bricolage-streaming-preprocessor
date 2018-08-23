package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.processor.Cleanse;
import lombok.*;

public class BigIntOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("bigint", (def) ->
            new BigIntOp(def)
        );
    }

    BigIntOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        return Long.valueOf(i);
    }
}
