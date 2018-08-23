package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BigintColumnProcessorTest {
    BigintColumnProcessor defaultProcessor() {
        return new BigintColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val f = defaultProcessor();
        assertEquals(Long.valueOf(1234567890123L), f.processValue(Long.valueOf(1234567890123L)));
        assertEquals(Long.valueOf(1), f.processValue(Integer.valueOf(1)));
        assertEquals(Long.valueOf(1), f.processValue("1"));
        assertNull(f.processValue(null));
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_1() throws Exception {
        val f = defaultProcessor();
        f.processValue("junk value");
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_2() throws Exception {
        val f = defaultProcessor();
        f.processValue(new Object());
    }
}
