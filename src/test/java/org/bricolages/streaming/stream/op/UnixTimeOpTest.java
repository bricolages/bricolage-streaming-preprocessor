package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.bricolages.streaming.stream.processor.CleanseException;
import java.time.ZoneOffset;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class UnixTimeOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void build() throws Exception {
        val def = new OperatorDefinition("unixtime", "schema.table", "ut_col", "{\"zoneOffset\":\"+09:00\"}");
        val op = (UnixTimeOp)builder.build(def);
        assertEquals("ut_col", op.targetColumnName());
        assertEquals(ZoneOffset.of("+09:00"), op.zoneOffset);
    }

    @Test
    public void apply() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue(1467358866, null));
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue("1467358866", null));
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue(Double.valueOf(1467358866), null));

        assertEquals("2016-07-01T16:41:06.246+09:00", f.applyValue(Double.valueOf(1467358866.246D), null));
        assertEquals("2016-07-01T16:41:06.246+09:00", f.applyValue("1467358866.246", null));
    }

    @Test(expected = CleanseException.class)
    public void apply_invalid() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        f.applyValue("junk value", null);
    }

    @Test(expected = CleanseException.class)
    public void apply_unsupported() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        f.applyValue(new Object(), null);
    }
}
