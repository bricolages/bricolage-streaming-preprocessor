package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class IntOpTest {
    @Test
    public void apply() throws Exception {
        val f = new IntOp(null);
        assertEquals(Integer.valueOf(1), f.applyValue(Integer.valueOf(1), null));
        assertEquals(Integer.valueOf(1), f.applyValue("1", null));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new IntOp(null);
        f.applyValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new IntOp(null);
        f.applyValue(new Object(), null);
    }
}
