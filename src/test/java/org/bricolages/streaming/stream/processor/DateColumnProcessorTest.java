package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DateColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "date");
    }

    @Test
    public void process() throws Exception {
        val proc = new DateColumnProcessor(col());
        assertNull(proc.processValue(null, null));
        assertEquals("2018-01-23", proc.processValue("2018-01-23", null));
    }

    @Test(expected = FilterException.class)
    public void process_parse_error() throws Exception {
        val proc = new DateColumnProcessor(col());
        proc.processValue("2018/01/23", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = new DateColumnProcessor(col());
        proc.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = new DateColumnProcessor(col());
        proc.processValue(new Object(), null);
    }
}
