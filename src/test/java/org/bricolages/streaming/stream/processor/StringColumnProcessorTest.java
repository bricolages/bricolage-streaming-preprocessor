package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.object.Record;
import org.bricolages.streaming.exception.*;
import java.util.ArrayList;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class StringColumnProcessorTest {
    @Test(expected=ConfigError.class)
    public void s_build() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "string";
        params.length = null;
        StringColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

    @Test
    public void create() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "string";
        params.length = 20;
        val proc = StreamColumn.forParams(params).buildProcessor(null);

        val rec = new Record();
        rec.put("x", "a\0");
        assertEquals("a", proc.process(rec));
    }

    StringColumnProcessor defaultProcessor() {
        return new StringColumnProcessor(StreamColumn.forName("x"), 20);
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(null));
        assertEquals("abc", proc.processValue("abc"));
        assertEquals("abc", proc.processValue("abc\0XXX"));
        assertEquals("aaaa|aaaa|aaa|aaaa|XXXXXX", proc.processValue("aaaa|aaaa|aaa|aaaa|XXXXXX"));
    }

    @Test
    public void process_non_string() throws Exception {
        val proc = defaultProcessor();

        assertEquals(Integer.valueOf(1), proc.processValue(Integer.valueOf(1)));

        val a = new ArrayList<Integer>();
        a.add(1);
        a.add(2);
        assertEquals(a, proc.processValue(a));
    }
}
