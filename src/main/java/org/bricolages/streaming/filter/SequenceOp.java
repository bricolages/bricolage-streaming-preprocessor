package org.bricolages.streaming.filter;

import org.bricolages.streaming.ApplicationError;
import org.bricolages.streaming.SequencialNumberRepository;
import lombok.*;

class SequenceOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("sequence", (def) ->
            new SequenceOp(def, builder.sequencialNumberRepository)
        );
    }

    long currentValue;
    long upperValue;

    SequenceOp(OperatorDefinition def) {
        super(def);
    }

    SequenceOp(OperatorDefinition def, SequencialNumberRepository repo) {
        this(def);
        val seq = repo.allocate();
        this.currentValue = seq.getLastValue();
        this.upperValue = seq.getNextValue();
    }

    private long getNextValue() throws FilterException {
        currentValue ++;
        if (currentValue > upperValue) {
            throw new FilterException("sequence number is starved");
        }
        return currentValue;
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        return getNextValue();
    }
}
