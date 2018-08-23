package org.bricolages.streaming.filter;
import org.bricolages.streaming.object.Record;
import java.time.ZoneOffset;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TimeZoneOpTest {
    OpBuilder builder = new OpBuilder();

    @Test
    public void build() throws Exception {
        val def = new OperatorDefinition("timezone", "schema.table", "tz_col", "{\"sourceOffset\":\"+00:00\",\"targetOffset\":\"+09:00\"}");
        val op = (TimeZoneOp)builder.build(def);
        assertEquals("tz_col", op.targetColumnName());
        assertEquals(ZoneOffset.of("+00:00"), op.sourceOffset);
        assertEquals(ZoneOffset.of("+09:00"), op.targetOffset);
    }

    @Test
    public void apply() throws Exception {
        val f = new TimeZoneOp(null, "+0000", "+0900", false);
        assertEquals("2016-07-01T19:41:06+09:00", f.applyValue("2016-07-01T10:41:06Z", null));
        assertEquals("2016-07-01T19:41:06+09:00", f.applyValue("2016-07-01T10:41:06+00:00", null));
        assertEquals("2016-07-01T19:41:06+09:00", f.applyValue("2016-07-01 10:41:06 +0000", null));
        assertEquals("2016-07-01T19:41:06+09:00", f.applyValue("2016-07-01 10:41:06 UTC", null));
        assertEquals("2016-07-01T19:41:06+09:00", f.applyValue("2016-07-01 10:41:06", null));

        // with fractional seconds
        assertEquals("2016-07-01T19:41:06.246+09:00", f.applyValue("2016-07-01T10:41:06.246Z", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", f.applyValue("2016-07-01T10:41:06.246+00:00", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", f.applyValue("2016-07-01 10:41:06.246 +0000", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", f.applyValue("2016-07-01 10:41:06.246 UTC", null));
    }

    @Test
    public void apply_trailing_tz() throws Exception {
        val f = new TimeZoneOp(null, "+0000", "+0900", true);
        assertEquals("2012-09-11T04:34:11+09:00", f.applyValue("2012-09-10T12:34:11-07:00[America/Los_Angeles]", null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new TimeZoneOp(null, "+0000", "+0900", false);
        f.applyValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new TimeZoneOp(null, "+0000", "+0900", false);
        f.applyValue(new Object(), null);
    }
}
