package org.bricolages.streaming.filter;

import org.bricolages.streaming.AllocatedRange;
import org.bricolages.streaming.SequencialNumberRepository;
import lombok.*;

class SequenceOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("sequence", (def) ->
            new SequenceOp(def, builder)
        );
    }

    SequencialNumberRepository sequencialNumberRepository;

    SequenceOp(OperatorDefinition def, OpBuilder builder) {
        super(def);
        this.sequencialNumberRepository = builder.sequencialNumberRepository;
    }

    static final long SEQUENCE_ID = 1; // FIXME: fixed magic number
    static final long ALLOCATION_SIZE = 10000;

    AllocatedRange range = AllocatedRange.none();
    private long getNextValue() {
        return range.getNextValue().orElseGet(() -> {
            range = sequencialNumberRepository.allocate(SEQUENCE_ID, ALLOCATION_SIZE);
            return getNextValue();
        }).longValue();
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        return getNextValue();
    }
}
