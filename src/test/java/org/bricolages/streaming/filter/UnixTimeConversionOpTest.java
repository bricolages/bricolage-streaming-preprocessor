package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class UnixTimeConversionOpTest {
    @Test
    public void testApply() throws Exception {
        val f = new UnixTimeConversionOp("+09:00");
        assertEquals("2016-07-01T16:41:06", f.apply(1467358866));
        assertEquals("2016-07-01T16:41:06", f.apply("1467358866"));
        assertEquals("2016-07-01T16:41:06", f.apply(Double.valueOf(1467358866)));
    }

    @Test(expected = FilterException.class)
    public void testForUrl_invalid() throws Exception {
        val f = new UnixTimeConversionOp("+09:00");
        f.apply("junk value");
    }

    @Test(expected = FilterException.class)
    public void testForUrl_unknown() throws Exception {
        val f = new UnixTimeConversionOp("+09:00");
        f.apply(new Object());
    }
}
