package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import java.util.ArrayList;
import static org.junit.Assert.*;
import lombok.*;

public class StringColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "string");
    }

    @Test
    public void process() throws Exception {
        StringColumnProcessor proc = new StringColumnProcessor(col(), 20);
        assertNull(proc.processValue(null, null));
        assertEquals("abc", proc.processValue("abc", null));
        assertEquals("abc", proc.processValue("abc\0XXX", null));
        assertEquals("aaaa|aaaa|aaa|aaaa|XXXXXX", proc.processValue("aaaa|aaaa|aaa|aaaa|XXXXXX", null));
    }

    @Test
    public void process_non_string() throws Exception {
        StringColumnProcessor proc = new StringColumnProcessor(col(), 20);

        assertEquals(Integer.valueOf(1), proc.processValue(Integer.valueOf(1), null));

        val a = new ArrayList<Integer>();
        a.add(1);
        a.add(2);
        assertEquals(a, proc.processValue(a, null));
    }
}
