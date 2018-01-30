package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DateColumnProcessorTest {
    DateColumnProcessor defaultProcessor() {
        return new DateColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(null));
        assertEquals("2018-01-23", proc.processValue("2018-01-23"));
    }

    @Test(expected = FilterException.class)
    public void process_parse_error() throws Exception {
        val proc = defaultProcessor();
        proc.processValue("2018/01/23");
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = defaultProcessor();
        proc.processValue("junk value");
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = defaultProcessor();
        proc.processValue(new Object());
    }
}
