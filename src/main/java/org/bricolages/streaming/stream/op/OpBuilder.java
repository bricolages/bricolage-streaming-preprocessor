package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.exception.ConfigError;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.BiFunction;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OpBuilder {
    public OpBuilder() {
        registerAll();
    }

    void registerAll() {
        IntOp.register(this);
        BigIntOp.register(this);
        TextOp.register(this);
        TimeZoneOp.register(this);
        UnixTimeOp.register(this);
        DeleteNullsOp.register(this);
        AggregateOp.register(this);
        DeleteOp.register(this);
        RenameOp.register(this);
        CollectRestOp.register(this);
        RejectOp.register(this);
        SequenceOp.register(this);
        DupOp.register(this);
        FloatOp.register(this);
    }

    private Map<String, BiFunction<OperatorDefinition, OpContext, Op>> builders = new HashMap<String, BiFunction<OperatorDefinition, OpContext, Op>>();

    public void registerOperator(String id, Function<OperatorDefinition, Op> builder) {
        registerOperator(id, (def, ctx) -> builder.apply(def));
    }

    public void registerOperator(String id, BiFunction<OperatorDefinition, OpContext, Op> builder) {
        log.debug("new operator builder registered: '{}' -> {}", id, builder);
        builders.put(id, builder);
    }

    final public Op build(OperatorDefinition def, OpContext ctx) {
        val builder = builders.get(def.getOperatorId());
        if (builder == null) {
            throw new ConfigError("unknown operator ID: " + def.getOperatorId());
        }
        return builder.apply(def, ctx);
    }
}
