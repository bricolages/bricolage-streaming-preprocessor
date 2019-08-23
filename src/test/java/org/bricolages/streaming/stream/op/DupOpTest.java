package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DupOpTest {
    TestOpBuilder builder = new TestOpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("dup", "schema.table", "b_dup", "{\"from\":\"b\"}");
        val op = (DupOp)builder.buildWithDefaultContext(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"b_dup\":2}", out.serialize());
    }
}
