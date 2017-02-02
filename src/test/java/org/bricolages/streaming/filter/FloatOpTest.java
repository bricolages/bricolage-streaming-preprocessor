package org.bricolages.streaming.filter;

import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class FloatOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("float", "schema.table", "b", "{}");
        val op = (FloatOp) builder.build(def);
        {
            val rec = Record.parse("{\"a\":1,\"b\":1.5,\"c\":3}");
            val out = op.apply(rec);
            assertEquals("{\"a\":1,\"b\":1.5,\"c\":3}", out.serialize());
        }
        {
            val rec = Record.parse("{\"a\":1,\"b\":\"1.5\",\"c\":3}");
            val out = op.apply(rec);
            assertEquals("{\"a\":1,\"b\":1.5,\"c\":3}", out.serialize());
        }
        {
            val rec = Record.parse("{\"a\":1,\"b\":\"1.234e2\",\"c\":3}");
            val out = op.apply(rec);
            assertEquals("{\"a\":1,\"b\":123.4,\"c\":3}", out.serialize());
        }
    }

    @Test
    public void apply_too_large_value() throws Exception {
        val f = new FloatOp(null);
        assertEquals(null, f.applyValue(Double.MAX_VALUE, null));
    }

    @Test
    public void apply_too_small_value() throws Exception {
        val f = new FloatOp(null);
        assertEquals(null, f.applyValue(Double.MIN_VALUE, null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new FloatOp(null);
        f.applyValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new FloatOp(null);
        f.applyValue(new Object(), null);
    }
}
