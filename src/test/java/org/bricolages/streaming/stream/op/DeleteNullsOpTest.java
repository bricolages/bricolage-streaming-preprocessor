package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DeleteNullsOpTest {
    TestOpBuilder builder = new TestOpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("deletenulls", "schema.table", "*", "{}");
        val op = (DeleteNullsOp)builder.buildWithDefaultContext(def);
        val rec = Record.parse("{\"a\":null,\"b\":1,\"c\":null}");
        val out = op.apply(rec);
        assertEquals("{\"b\":1}", out.serialize());
    }
}
