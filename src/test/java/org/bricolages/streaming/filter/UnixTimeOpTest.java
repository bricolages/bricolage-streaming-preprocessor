package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class UnixTimeOpTest {
    @Test
    public void apply() throws Exception {
        val f = new UnixTimeOp("+0900");
        assertEquals("2016-07-01T16:41:06+09:00", f.apply(1467358866));
        assertEquals("2016-07-01T16:41:06+09:00", f.apply("1467358866"));
        assertEquals("2016-07-01T16:41:06+09:00", f.apply(Double.valueOf(1467358866)));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new UnixTimeOp("+0900");
        f.apply("junk value");
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new UnixTimeOp("+0900");
        f.apply(new Object());
    }
}
