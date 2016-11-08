package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class AggregateOpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\"}");
        val op = (AggregateOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }

    @Test
    public void apply_no_remove() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\",\"dropTargetColumns\":false}");
        val op = (AggregateOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }
}
