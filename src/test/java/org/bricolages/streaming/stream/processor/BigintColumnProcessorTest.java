package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class BigintColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "bigint");
    }

    @Test
    public void process() throws Exception {
        val f = new BigintColumnProcessor(col());
        assertEquals(Long.valueOf(1234567890123L), f.processValue(Long.valueOf(1234567890123L), null));
        assertEquals(Long.valueOf(1), f.processValue(Integer.valueOf(1), null));
        assertEquals(Long.valueOf(1), f.processValue("1", null));
        assertNull(f.processValue(null, null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val f = new BigintColumnProcessor(col());
        f.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val f = new BigintColumnProcessor(col());
        f.processValue(new Object(), null);
    }
}
