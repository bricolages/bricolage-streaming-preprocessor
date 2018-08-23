package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DoubleColumnProcessorTest {
    DoubleColumnProcessor defaultProcessor() {
        return new DoubleColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertEquals(Double.valueOf((double)1.23F), proc.processValue(Float.valueOf(1.23F)));
        assertEquals(Double.valueOf((double)-1.23F), proc.processValue(Float.valueOf(-1.23F)));
        assertEquals(Double.valueOf(1.23D), proc.processValue(Double.valueOf(1.23D)));
        assertEquals(Double.valueOf(-1.23D), proc.processValue(Double.valueOf(-1.23D)));
        assertEquals(Double.valueOf(1.23D), proc.processValue("1.23"));
        assertEquals(Double.valueOf(-1.23D), proc.processValue("-1.23"));
        assertEquals(Double.valueOf(123.4D), proc.processValue("1.234e2"));
    }

    @Test
    public void process_inf() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(Double.POSITIVE_INFINITY));
    }

    @Test
    public void process_nan() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(Double.NaN));
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_1() throws Exception {
        val proc = defaultProcessor();
        proc.processValue("junk value");
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_2() throws Exception {
        val proc = defaultProcessor();
        proc.processValue(new Object());
    }
}
