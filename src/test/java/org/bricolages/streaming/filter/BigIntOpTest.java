package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BigIntOpTest {
    @Test
    public void apply() throws Exception {
        val f = new BigIntOp();
        assertEquals(Long.valueOf(1234567890123L), f.apply(Long.valueOf(1234567890123L)));
        assertEquals(Long.valueOf(1), f.apply(Integer.valueOf(1)));
        assertEquals(Long.valueOf(1), f.apply("1"));
        assertNull(f.apply(null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new BigIntOp();
        f.apply("junk value");
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new BigIntOp();
        f.apply(new Object());
    }
}
