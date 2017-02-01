package org.bricolages.streaming.filter;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import org.bricolages.streaming.ConfigError;
import org.bricolages.streaming.SequencialNumberRepository;

import lombok.*;

@Slf4j
public class OpBuilder {
    public final SequencialNumberRepository sequencialNumberRepository;

    public OpBuilder() {
        this(null);
    }
    public OpBuilder(SequencialNumberRepository repo) {
        this.sequencialNumberRepository = repo;
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
    }

    private Map<String, Function<OperatorDefinition, Op>> builders = new HashMap<String, Function<OperatorDefinition, Op>>();
    public void registerOperator(String id, Function<OperatorDefinition, Op> builder) {
        log.debug("new operator builder registered: '{}' -> {}", id, builder);
        builders.put(id, builder);
    }

    final public Op build(OperatorDefinition def) {
        val builder = builders.get(def.getOperatorId());
        if (builder == null) {
            throw new ConfigError("unknown operator ID: " + def.getOperatorId());
        }
        return builder.apply(def);
    }
}