package org.bricolages.streaming.stream.op;
import lombok.*;

public class TestOpBuilder extends OpBuilder {
    @RequiredArgsConstructor
    static class OpContextImpl implements OpContext {
        @Getter final String streamPrefix;
        @Getter SequencialNumberRepository sequencialNumberRepository;
    }

    OpContextImpl ctx;

    public TestOpBuilder() {
        super();
        this.ctx = new OpContextImpl(null);
    }

    public TestOpBuilder(String streamPrefix) {
        super();
        this.ctx = new OpContextImpl(streamPrefix);
    }

    public Op buildWithDefaultContext(OperatorDefinition def) {
        return build(def, ctx);
    }
}
