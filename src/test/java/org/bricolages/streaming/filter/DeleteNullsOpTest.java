package org.bricolages.streaming.filter;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DeleteNullsOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("deletenulls", "schema.table", "*", "{}");
        val op = (DeleteNullsOp)builder.build(def);
        val rec = Record.parse("{\"a\":null,\"b\":1,\"c\":null}");
        val out = op.apply(rec);
        assertEquals("{\"b\":1}", out.serialize());
    }
}
