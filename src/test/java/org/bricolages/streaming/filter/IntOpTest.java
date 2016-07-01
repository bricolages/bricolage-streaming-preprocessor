package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class IntOpTest {
    @Test
    public void apply() throws Exception {
        val f = new IntOp();
        assertEquals(Integer.valueOf(1), f.apply(Integer.valueOf(1)));
        assertEquals(Integer.valueOf(1), f.apply("1"));
    }

    @Test(expected = FilterException.class)
    public void apply_invalid() throws Exception {
        val f = new IntOp();
        f.apply("junk value");
    }

    @Test(expected = FilterException.class)
    public void apply_unsupported() throws Exception {
        val f = new IntOp();
        f.apply(new Object());
    }
}
