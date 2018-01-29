package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DoubleColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "double");
    }

    @Test
    public void process() throws Exception {
        val proc = new DoubleColumnProcessor(col());
        assertEquals(Double.valueOf((double)1.23F), proc.processValue(Float.valueOf(1.23F), null));
        assertEquals(Double.valueOf((double)-1.23F), proc.processValue(Float.valueOf(-1.23F), null));
        assertEquals(Double.valueOf(1.23D), proc.processValue(Double.valueOf(1.23D), null));
        assertEquals(Double.valueOf(-1.23D), proc.processValue(Double.valueOf(-1.23D), null));
        assertEquals(Double.valueOf(1.23D), proc.processValue("1.23", null));
        assertEquals(Double.valueOf(-1.23D), proc.processValue("-1.23", null));
        assertEquals(Double.valueOf(123.4D), proc.processValue("1.234e2", null));
    }

    @Test
    public void process_inf() throws Exception {
        val proc = new DoubleColumnProcessor(col());
        assertNull(proc.processValue(Double.POSITIVE_INFINITY, null));
    }

    @Test
    public void process_nan() throws Exception {
        val proc = new DoubleColumnProcessor(col());
        assertNull(proc.processValue(Double.NaN, null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = new DoubleColumnProcessor(col());
        proc.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = new DoubleColumnProcessor(col());
        proc.processValue(new Object(), null);
    }
}
