package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.processor.Cleanse;
import org.bricolages.streaming.stream.processor.CleanseException;
import org.bricolages.streaming.object.Record;
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
    public Object applyValue(Object value, Record record) throws FilterException, CleanseException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        return Long.valueOf(i);
    }
}
