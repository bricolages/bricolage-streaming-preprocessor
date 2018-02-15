package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.Record;
import org.bricolages.streaming.filter.FilterException;
import org.bricolages.streaming.exception.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectColumnProcessorTest {
    @Test(expected=ConfigError.class)
    public void s_build() throws Exception {
        val params = new StreamColumn.Params();
        params.name = "x";
        params.type = "object";
        params.length = null;
        ObjectColumnProcessor.build(StreamColumn.forParams(params), new NullContext());
    }

    ObjectColumnProcessor defaultProcessor() {
        return new ObjectColumnProcessor(StreamColumn.forName("x"), 20);
    }

    Object object(Object... kvs) {
        if (kvs.length % 2 != 0) {
            throw new RuntimeException("bad key-value pair (length % 2 != 0)");
        }
        val obj = new HashMap<Object, Object>();
        for (int i = 0; i < kvs.length; i += 2) {
            Object key = kvs[i];
            Object val = kvs[i+1];
            obj.put(key, val);
        }
        return obj;
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(null));
        assertEquals(object("x", 1), proc.processValue(object("x", 1)));

        // {"x":""} is 8 characters (bytes), rest is 12 bytes
        assertEquals(object("x", "aaaa|aaaa|12"), proc.processValue(object("x", "aaaa|aaaa|12")));
    }

    @Test(expected=FilterException.class)
    public void process_too_long() throws Exception {
        val proc = defaultProcessor();
        assertNull(proc.processValue(object("x", "aaaa|aaaa|123")));
    }

    @Test
    public void process_json_string() throws Exception {
        val proc = defaultProcessor();
        assertEquals(object("x", 1), proc.processValue("{\"x\":1}"));
        assertEquals("string", proc.processValue("string"));
    }

    @Test
    public void process_non_object() throws Exception {
        val proc = defaultProcessor();
        assertEquals(Integer.valueOf(1), proc.processValue(Integer.valueOf(1)));

        val a = new ArrayList<Integer>();
        a.add(1);
        a.add(2);
        assertEquals(a, proc.processValue(a));
    }
}
