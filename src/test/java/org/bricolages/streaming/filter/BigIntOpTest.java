package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BigIntOpTest {
    @Test
    public void apply() throws Exception {
        val f = new BigIntOp(null);
        assertEquals(Long.valueOf(1234567890123L), f.applyValue(Long.valueOf(1234567890123L), null));
        assertEquals(Long.valueOf(1), f.applyValue(Integer.valueOf(1), null));
        assertEquals(Long.valueOf(1), f.applyValue("1", null));
        assertNull(f.applyValue(null, null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new BigIntOp(null);
        f.applyValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new BigIntOp(null);
        f.applyValue(new Object(), null);
    }
}
