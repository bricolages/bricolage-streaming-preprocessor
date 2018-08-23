package org.bricolages.streaming.filter;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class AggregateOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\"}");
        val op = (AggregateOp)builder.build(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }

    @Test
    public void apply_keepTargetColumns() throws Exception {
        val def = new OperatorDefinition("aggregate", "schema.table", "*", "{\"targetColumns\":\"^q:\",\"aggregatedColumn\":\"q\",\"keepTargetColumns\":true}");
        val op = (AggregateOp)builder.build(def);
        val rec = Record.parse("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"q:x\":2,\"q:y\":3,\"b\":4,\"q\":{\"x\":2,\"y\":3}}", out.serialize());
    }
}
