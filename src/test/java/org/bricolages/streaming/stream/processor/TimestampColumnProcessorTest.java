package org.bricolages.streaming.stream.processor;

import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.exception.*;

import java.util.concurrent.TimeUnit;

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
        params.timeUnit = null;
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }
    */

    @Test(expected=ConfigError.class)
    public void s_build_err2() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+00:00";
        params.sourceOffset = null;  // must not be null
        params.timeUnit = null;
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

    @Test(expected=ConfigError.class)
    public void s_build_err3() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+09:00";
        params.sourceOffset = "+00:00";
        params.timeUnit = "minutes";    // is not supported
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

    @Test(expected=ConfigError.class)
    public void s_build_err4() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+09:00";
        params.sourceOffset = "+00:00";
        params.timeUnit = "microseconds";    // is not supported
        TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

    @Test
    public void s_build_timeunit_default() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+09:00";
        params.sourceOffset = "+00:00";
        params.timeUnit = null;
        val proc = TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
        assertEquals(TimeUnit.SECONDS, proc.getTimeUnit());
    }

    @Test
    public void s_build_timeunit_milli() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "timestamp";
        params.zoneOffset = "+09:00";
        params.sourceOffset = "+00:00";
        params.timeUnit = "milliseconds";
        val proc = TimestampColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
        assertEquals(TimeUnit.MILLISECONDS, proc.getTimeUnit());
    }

    TimestampColumnProcessor defaultProcessor() {
        return new TimestampColumnProcessor(StreamColumn.forName("x"), "+00:00", "+09:00", TimeUnit.SECONDS);
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

        // milliseconds for seconds
        assertEquals("2020-07-15T18:53:53.123+09:00", proc.processValue(1594806833123L));
        assertEquals("2020-07-15T18:53:53.123+09:00", proc.processValue(Double.valueOf(1594806833123L)));
    }

    @Test
    public void process_unixtime_milliseconds() throws Exception {
        val proc = new TimestampColumnProcessor(StreamColumn.forName("x"), "+00:00", "+09:00", TimeUnit.MILLISECONDS);

        assertEquals("2016-07-01T16:41:06.123+09:00", proc.processValue(1467358866123L));
        assertEquals("2016-07-01T16:41:06.123+09:00", proc.processValue("1467358866123"));
        assertEquals("2016-07-01T16:41:06.123+09:00", proc.processValue(Double.valueOf(1467358866123D)));

        // with fractional seconds: 3 digits is max
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue(Double.valueOf(1467358866246.4D)));
        assertEquals("2016-07-01T16:41:06.246+09:00", proc.processValue("1467358866246.4"));
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_1() throws Exception {
        val proc = defaultProcessor();
        proc.processValue("junk value");
    }

    @Test(expected = ProcessorException.class)
    public void process_inval_2() throws Exception {
        val proc = defaultProcessor();
        proc.processValue(new Object());
    }

    @Test
    public void process_timestamp_asis_offset() throws Exception {
        val proc = new TimestampColumnProcessor(StreamColumn.forName("x"), "+00:00", null, TimeUnit.SECONDS);
        assertEquals("2016-07-01T10:41:06+07:00", proc.processValue("2016-07-01T10:41:06+07:00"));
        assertEquals("2016-07-01T10:41:06+07:00", proc.processValue("2016-07-01T10:41:06+0700"));
        assertEquals("2016-07-01T10:41:06.251+07:00", proc.processValue("2016-07-01T10:41:06.251+07:00"));
        assertEquals("2016-07-01T10:41:06.251+07:00", proc.processValue("2016-07-01T10:41:06.251+0700"));
        // fractional '0' disappears
        assertEquals("2016-07-01T10:41:06.25+07:00", proc.processValue("2016-07-01T10:41:06.250+07:00"));
    }
}
