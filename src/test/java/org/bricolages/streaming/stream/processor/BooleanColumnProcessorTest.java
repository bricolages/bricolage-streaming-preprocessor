package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BooleanColumnProcessorTest {
    BooleanColumnProcessor defaultProcessor() {
        return new BooleanColumnProcessor(StreamColumn.forName("col0"));
    }

    @Test
    public void process() throws Exception {
        val f = defaultProcessor();
        assertEquals(Boolean.valueOf(true), f.processValue(Boolean.valueOf(true)));
        assertEquals(Boolean.valueOf(false), f.processValue(Boolean.valueOf(false)));
        assertEquals(Boolean.valueOf(true), f.processValue("true"));
        assertEquals(Boolean.valueOf(false), f.processValue("false"));
        assertEquals(Boolean.valueOf(true), f.processValue("t"));
        assertEquals(Boolean.valueOf(false), f.processValue("f"));
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
