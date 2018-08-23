package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RealColumnProcessorTest {
    RealColumnProcessor defaultProcessor() {
        return new RealColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertEquals(Float.valueOf(1.23F), proc.processValue(Float.valueOf(1.23F)));
        assertEquals(Float.valueOf(-1.23F), proc.processValue(Float.valueOf(-1.23F)));
        assertEquals(Float.valueOf(1.23F), proc.processValue(Double.valueOf(1.23D)));
        assertEquals(Float.valueOf(-1.23F), proc.processValue(Double.valueOf(-1.23D)));
        assertEquals(Float.valueOf(1.23F), proc.processValue("1.23"));
        assertEquals(Float.valueOf(-1.23F), proc.processValue("-1.23"));
        assertEquals(Float.valueOf(123.4F), proc.processValue("1.234e2"));
    }

    @Test
    public void apply_too_large_value() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue("440282346638528860000000000000000000000.0"));
    }

    @Test
    public void apply_too_small_value() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue("-440282346638528860000000000000000000000.0"));
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
