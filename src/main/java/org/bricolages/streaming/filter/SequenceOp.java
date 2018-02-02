package org.bricolages.streaming.filter;
import org.bricolages.streaming.exception.ApplicationError;
import lombok.*;

public class SequenceOp extends SingleColumnOp {
    static final void register(OpBuilder builder) {
        builder.registerOperator("sequence", (def) ->
            new SequenceOp(def, new SequencialNumberAllocatorImpl(builder.sequencialNumberRepository))
        );
    }

    static class SequencialNumberAllocatorImpl implements SequencialNumberAllocator {
        final SequencialNumberRepository repos;

        SequencialNumberAllocatorImpl(SequencialNumberRepository repos) {
            this.repos = repos;
        }

        public SequencialNumber allocate() {
            return this.repos.allocate();
        }
    }

    final SequencialNumberAllocator seqRepo;
    long currentValue;
    long upperValue;

    SequenceOp(OperatorDefinition def, SequencialNumberAllocator repo) {
        super(def);
        this.seqRepo = repo;
        allocateSequenceBlock();
    }

    void allocateSequenceBlock() {
        val seq = seqRepo.allocate();
        this.currentValue = seq.getLastValue();
        this.upperValue = seq.getNextValue();
    }

    private long getNextValue() {
        // We get a sequence block like [current=1000, upper=2000].
        // We should use 1001-2000 for that block, so increment currentValue first.
        currentValue ++;

        // We can use the last value of the block (e.g. 2000)
        if (currentValue > upperValue) {
            allocateSequenceBlock();
            // Do not use the first number of the block (e.g. 2000)
            currentValue ++;
        }

        return this.currentValue;
    }

    @Override
    public Object applyValue(Object value, Record record) throws FilterException {
        return getNextValue();
    }
}
