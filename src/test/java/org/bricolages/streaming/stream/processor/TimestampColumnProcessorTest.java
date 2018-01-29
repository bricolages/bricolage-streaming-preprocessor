package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TimestampColumnProcessorTest {
    TimestampColumnProcessor makeProc() {
        val col = new StreamColumn("col0", "timestamp");
        return new TimestampColumnProcessor(col, "+00:00", "+09:00");
    }

    @Test
    public void process_timestamp_string() throws Exception {
        val proc = makeProc();

        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01T10:41:06Z", null));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01T10:41:06+00:00", null));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01 10:41:06 +0000", null));
        assertEquals("2016-07-01T19:41:06+09:00", proc.processValue("2016-07-01 10:41:06 UTC", null));

        // with fractional seconds
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01T10:41:06.246Z", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01T10:41:06.246+00:00", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01 10:41:06.246 +0000", null));
        assertEquals("2016-07-01T19:41:06.246+09:00", proc.processValue("2016-07-01 10:41:06.246 UTC", null));

        // trailing TZ
        assertEquals("2012-09-11T04:34:11+09:00", proc.processValue("2012-09-10T12:34:11-07:00[America/Los_Angeles]", null));
    }

    @Test
    public void process_unixtime() throws Exception {
        val proc = makeProc();

        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue(1467358866, null));
        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue("1467358866", null));
        assertEquals("2016-07-01T16:41:06+09:00", proc.processValue(Double.valueOf(1467358866), null));

        // with fractional seconds
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue(Double.valueOf(1467358866.246D), null));
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue("1467358866.246", null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = makeProc();
        proc.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = makeProc();
        proc.processValue(new Object(), null);
    }
}
