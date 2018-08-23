package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.stream.processor.Cleanse;
import org.bricolages.streaming.stream.processor.CleanseException;
import org.bricolages.streaming.object.Record;
import lombok.*;

class IntOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("int", (def) ->
            new IntOp(def)
        );
    }

    IntOp(OperatorDefinition def) {
        super(def);
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException, CleanseException {
        if (value == null) return null;
        long i = Cleanse.getInteger(value);
        if (Integer.MIN_VALUE <= i && i <= Integer.MAX_VALUE) {
            return Integer.valueOf((int)i);
        }
        else {
            return null;
        }
    }
}
