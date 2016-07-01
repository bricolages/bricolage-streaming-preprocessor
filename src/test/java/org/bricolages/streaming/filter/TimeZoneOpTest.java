package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TimeZoneOpTest {
    @Test
    public void apply() throws Exception {
        val f = new TimeZoneOp("+0000", "+0900");
        assertEquals("2016-07-01T19:41:06+09:00", f.apply("2016-07-01T10:41:06Z"));
        assertEquals("2016-07-01T19:41:06+09:00", f.apply("2016-07-01T10:41:06+00:00"));
        assertEquals("2016-07-01T19:41:06+09:00", f.apply("2016-07-01 10:41:06 +0000"));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new TimeZoneOp("+0000", "+0900");
        f.apply("junk value");
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new TimeZoneOp("+0000", "+0900");
        f.apply(new Object());
    }
}
