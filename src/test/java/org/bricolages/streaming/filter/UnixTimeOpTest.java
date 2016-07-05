package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class UnixTimeOpTest {
    @Test
    public void apply() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue(1467358866, null));
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue("1467358866", null));
        assertEquals("2016-07-01T16:41:06+09:00", f.applyValue(Double.valueOf(1467358866), null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        f.applyValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new UnixTimeOp(null, "+0900");
        f.applyValue(new Object(), null);
    }
}
