package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RemoveNullOpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition(0, "removenull", "schema.table", "*", "{}");
        val op = (RemoveNullOp)Op.build(def);
        val rec = Record.parse("{\"a\":null,\"b\":1,\"c\":null}");
        val out = op.apply(rec);
        assertEquals("{\"b\":1}", out.serialize());
    }
}
