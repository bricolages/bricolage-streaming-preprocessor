package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DeleteNullsOpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("deletenulls", "schema.table", "*", "{}");
        val op = (DeleteNullsOp)Op.build(def);
        val rec = Record.parse("{\"a\":null,\"b\":1,\"c\":null}");
        val out = op.apply(rec);
        assertEquals("{\"b\":1}", out.serialize());
    }
}
