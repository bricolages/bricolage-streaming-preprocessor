package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class CollectRestOpTest {
    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("collectrest", "schema.table", "*", "{\"rejectColumns\":[\"a\",\"c\"],\"aggregatedColumn\":\"rest\"}");
        val op = (CollectRestOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"d\":4}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"c\":3,\"rest\":{\"b\":2,\"d\":4}}", out.serialize());
    }
}
