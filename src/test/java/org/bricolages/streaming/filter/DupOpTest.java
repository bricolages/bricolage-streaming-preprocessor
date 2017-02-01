package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DupOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("dup", "schema.table", "b_dup", "{\"from\":\"b\"}");
        val op = (DupOp)builder.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"b_dup\":2}", out.serialize());
    }
}
