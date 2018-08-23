package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RenameOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("rename", "schema.table", "b", "{\"to\":\"b_renamed\"}");
        val op = (RenameOp)builder.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"c\":3,\"b_renamed\":2}", out.serialize());
    }
}
