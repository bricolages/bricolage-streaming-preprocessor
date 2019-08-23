package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class AggregateOpTest {
    TestOpBuilder builder = new TestOpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\"}");
        val op = (AggregateOp)builder.buildWithDefaultContext(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }

    @Test
    public void apply_keepTargetColumns() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\",\"keepTargetColumns\":true}");
        val op = (AggregateOp)builder.buildWithDefaultContext(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }
}
