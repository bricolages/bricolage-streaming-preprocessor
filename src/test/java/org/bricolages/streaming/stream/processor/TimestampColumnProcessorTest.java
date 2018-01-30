package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TimestampColumnProcessorTest {
    TimestampColumnProcessor defaultProcessor() {
        return new TimestampColumnProcessor(StreamColumn.forName("x"), "+00:00", "+09:00");
    }

    @Test
    public void process_timestamp_string() throws Exception {
        val proc = defaultProcessor();

        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01T10:41:06Z"));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01T10:41:06+00:00"));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01 10:41:06 +0000"));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01 10:41:06 UTC"));

        // with fractional seconds
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01T10:41:06.246Z"));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01T10:41:06.246+00:00"));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01 10:41:06.246 +0000"));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01 10:41:06.246 UTC"));

        // trailing TZ
        assertEquals("2012-09-11T04:34:11+09:00", proc.processValue("2012-09-10T12:34:11-07:00[America/Los_Angeles]"));
    }

    @Test
    public void process_unixtime() throws Exception {
        val proc = defaultProcessor();

        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue(1467358866));
        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue("1467358866"));
        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue(Double.valueOf(1467358866)));

        // with fractional seconds
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue(Double.valueOf(1467358866.246D)));
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue("1467358866.246"));
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
