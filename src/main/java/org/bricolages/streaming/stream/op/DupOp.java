package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import lombok.*;

public class DupOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("dup", (def) ->
            new DupOp(def, def.mapParameters(Parameters.class))
        );
    }

    String originalName;

    @Getter
    @Setter
    public static class Parameters {
        String from;
    }

    DupOp(OperatorDefinition def, Parameters params) {
        this(def, params.from);
    }
    DupOp(OperatorDefinition def, String originalName) {
        super(def);
        this.originalName = originalName;
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        Object originalValue = record.get(originalName);
        return originalValue;
    }
}
