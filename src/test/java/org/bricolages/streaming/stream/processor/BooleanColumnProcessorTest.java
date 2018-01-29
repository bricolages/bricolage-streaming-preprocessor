package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BooleanColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "boolean");
    }

    @Test
    public void process() throws Exception {
        val f = new BooleanColumnProcessor(col());
        assertEquals(Boolean.valueOf(true), f.processValue(Boolean.valueOf(true), null));
        assertEquals(Boolean.valueOf(false), f.processValue(Boolean.valueOf(false), null));
        assertEquals(Boolean.valueOf(true), f.processValue("true", null));
        assertEquals(Boolean.valueOf(false), f.processValue("false", null));
        assertEquals(Boolean.valueOf(true), f.processValue("t", null));
        assertEquals(Boolean.valueOf(false), f.processValue("f", null));
        assertNull(f.processValue(null, null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val f = new BooleanColumnProcessor(col());
        f.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val f = new BooleanColumnProcessor(col());
        f.processValue(new Object(), null);
    }
}
