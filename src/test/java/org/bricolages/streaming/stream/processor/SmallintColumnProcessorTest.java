package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class SmallintColumnProcessorTest {
    SmallintColumnProcessor defaultProcessor() {
        return new SmallintColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertEquals(Short.valueOf((short)1), proc.processValue(Short.valueOf((short)1)));
        assertEquals(Short.valueOf((short)1), proc.processValue("1"));

        assertEquals(Short.valueOf(Short.MAX_VALUE), proc.processValue(Short.valueOf(Short.MAX_VALUE)));
        assertNull(proc.processValue(Integer.valueOf(((int)Short.MAX_VALUE) + 1)));
        assertEquals(Short.valueOf(Short.MIN_VALUE), proc.processValue(Short.valueOf(Short.MIN_VALUE)));
        assertNull(proc.processValue(Integer.valueOf(((int)Short.MIN_VALUE) - 1)));
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
