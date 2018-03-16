package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class DateColumnProcessorTest {
    DateColumnProcessor defaultProcessor() {
        return new DateColumnProcessor(StreamColumn.forName("x"), "+00:00", null);
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(null));
        assertEquals("2018-01-23", proc.processValue("2018-01-23"));
    }

    @Test
    public void process_timestamp() throws Exception {
        val proc = defaultProcessor();
        assertEquals("2018-01-23", proc.processValue("2018-01-23 12:00:00"));
        assertEquals("2018-01-23", proc.processValue("2018-01-23T03:00:00+00:00"));
        assertEquals("2018-01-23", proc.processValue("2018-01-23T03:00:00+09:00"));
    }

    @Test
    public void process_timestamp_tz() throws Exception {
        val proc = new DateColumnProcessor(StreamColumn.forName("x"), "+00:00", "+09:00");
        assertEquals("2018-01-24", proc.processValue("2018-01-23 23:00:00"));
        assertEquals("2018-01-24", proc.processValue("2018-01-23T23:00:00+00:00"));
        assertEquals("2018-01-23", proc.processValue("2018-01-23T23:00:00+09:00"));
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
