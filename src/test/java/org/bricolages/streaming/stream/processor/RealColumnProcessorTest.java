package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RealColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "real");
    }

    @Test
    public void process() throws Exception {
        val proc = new RealColumnProcessor(col());
        assertEquals(Float.valueOf(1.23F), proc.processValue(Float.valueOf(1.23F), null));
        assertEquals(Float.valueOf(-1.23F), proc.processValue(Float.valueOf(-1.23F), null));
        assertEquals(Float.valueOf(1.23F), proc.processValue(Double.valueOf(1.23D), null));
        assertEquals(Float.valueOf(-1.23F), proc.processValue(Double.valueOf(-1.23D), null));
        assertEquals(Float.valueOf(1.23F), proc.processValue("1.23", null));
        assertEquals(Float.valueOf(-1.23F), proc.processValue("-1.23", null));
        assertEquals(Float.valueOf(123.4F), proc.processValue("1.234e2", null));
    }

    @Test
    public void apply_too_large_value() throws Exception {
        val proc = new RealColumnProcessor(col());
        assertNull(proc.processValue("440282346638528860000000000000000000000.0", null));
    }

    @Test
    public void apply_too_small_value() throws Exception {
        val proc = new RealColumnProcessor(col());
        assertNull(proc.processValue("-440282346638528860000000000000000000000.0", null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = new RealColumnProcessor(col());
        proc.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = new RealColumnProcessor(col());
        proc.processValue(new Object(), null);
    }
}
