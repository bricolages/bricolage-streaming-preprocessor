package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import org.bricolages.streaming.exception.ApplicationError;
import static org.junit.Assert.*;
import lombok.*;

public class SequenceOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val repos = new DummyRepository();
        val op = new SequenceOp(def, repos);
        op.currentValue = 9;
        op.upperValue = 10;
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out.serialize());
    }

    public void apply_twice() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val repos = new DummyRepository();
        val op = new SequenceOp(def, repos);
        op.currentValue = 9;
        op.upperValue = 11;
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out.serialize());
        val out2 = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":11}", out2.serialize());
    }

    public void apply_over_capacity() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val repos = new DummyRepository();
        val op = new SequenceOp(def, repos);  // current=0, upper=10
        op.currentValue = 9;                  // current=9, upper=10
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");

        val out1 = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out1.serialize());
        assertEquals(10, op.currentValue);
        assertEquals(10, op.upperValue);

        val out2 = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":11}", out2.serialize());
        assertEquals(11, op.currentValue);
        assertEquals(20, op.upperValue);

        val out3 = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":12}", out3.serialize());
        assertEquals(12, op.currentValue);
        assertEquals(20, op.upperValue);
    }

    static class DummyRepository implements SequencialNumberAllocator {
        long last = 0;

        static final long BLOCK_SIZE = 10;

        public SequencialNumber allocate() {
            long curr = this.last;
            this.last += BLOCK_SIZE;
            long last = this.last;
            return new SequencialNumber(curr, last);
        }
    }
}
