package org.bricolages.streaming.filter;
import org.bricolages.streaming.object.Record;
import org.bricolages.streaming.stream.processor.CleanseException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class FloatOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("float", "schema.table", "n", "{}");
        val op = (FloatOp) builder.build(def);
        {
            val rec = Record.parse("{\"n\":1.5}");
            val out = op.apply(rec);
            assertEquals("{\"n\":1.5}", out.serialize());
        }
        {
            val rec = Record.parse("{\"n\":\"1.5\"}");
            val out = op.apply(rec);
            assertEquals("{\"n\":1.5}", out.serialize());
        }
        {
            val rec = Record.parse("{\"n\":\"1.234e2\"}");
            val out = op.apply(rec);
            assertEquals("{\"n\":123.4}", out.serialize());
        }
        {
            val rec = Record.parse("{\"n\":1.234e2}");
            val out = op.apply(rec);
            assertEquals("{\"n\":123.4}", out.serialize());
        }
        {
            val rec = Record.parse("{\"n\":-1.234}");
            val out = op.apply(rec);
            assertEquals("{\"n\":-1.234}", out.serialize());
        }
    }

    @Test
    public void apply_too_large_value() throws Exception {
        val f = new FloatOp(null);
        assertEquals(null, f.applyValue(440282346638528860000000000000000000000.0, null));
    }

    @Test
    public void apply_too_small_value() throws Exception {
        val f = new FloatOp(null);
        assertEquals(null, f.applyValue(-440282346638528860000000000000000000000.0, null));
    }

    @Test(expected = CleanseException.class)
    public void apply_invalid() throws Exception {
        val f = new FloatOp(null);
        f.applyValue("junk value", null);
    }

    @Test(expected = CleanseException.class)
    public void apply_unsupported() throws Exception {
        val f = new FloatOp(null);
        f.applyValue(new Object(), null);
    }
}
