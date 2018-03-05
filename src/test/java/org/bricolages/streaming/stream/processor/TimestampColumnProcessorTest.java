package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TimestampColumnProcessorTest {
    /** zoneOffset may be null now
    @Test(expected=ConfigError.class)
    public void s_build_err1() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = null;
        params.sourceOffset = "+00:00";
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }
    */

    @Test(expected=ConfigError.class)
    public void s_build_err2() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+00:00";
        params.sourceOffset = null;
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

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

    @Test
    public void process_timestamp_asis_offset() throws Exception {
        val proc = new TimestampColumnProcessor(StreamColumn.forName("x"), "+00:00", null);
        assertEquals("2016-07-01T10:41:06+07:00", proc.processValue("2016-07-01T10:41:06+07:00"));
        assertEquals("2016-07-01T10:41:06+07:00", proc.processValue("2016-07-01T10:41:06+0700"));
        assertEquals("2016-07-01T10:41:06.251+07:00", proc.processValue("2016-07-01T10:41:06.251+07:00"));
        assertEquals("2016-07-01T10:41:06.251+07:00", proc.processValue("2016-07-01T10:41:06.251+0700"));
        // fractional '0' disappears
        assertEquals("2016-07-01T10:41:06.25+07:00", proc.processValue("2016-07-01T10:41:06.250+07:00"));
    }
}
