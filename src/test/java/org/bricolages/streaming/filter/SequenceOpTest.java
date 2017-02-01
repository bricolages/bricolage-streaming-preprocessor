package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class SequenceOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val op = new SequenceOp(def);
        op.currentValue = 9;
        op.upperValue = 10;
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out.serialize());
    }

    public void apply_twice() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val op = new SequenceOp(def);
        op.currentValue = 9;
        op.upperValue = 11;
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out.serialize());
        val out2 = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":11}", out2.serialize());
    }

    @Test(expected=FilterException.class)
    public void apply_over_capacity() throws Exception {
        val def = new OperatorDefinition("sequence", "schema.table", "seq", "{}");
        val op = new SequenceOp(def);
        op.currentValue = 9;
        op.upperValue = 10;
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"seq\":10}", out.serialize());
        op.apply(rec);
    }
}
