package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class IntegerColumnProcessorTest {
    StreamColumn col() {
        return new StreamColumn("col0", "integer");
    }

    @Test
    public void process() throws Exception {
        val proc = new IntegerColumnProcessor(col());
        assertEquals(Integer.valueOf(1), proc.processValue(Integer.valueOf(1), null));
        assertEquals(Integer.valueOf(1), proc.processValue("1", null));
    }

    @Test(expected = FilterException.class)
    public void process_inval_1() throws Exception {
        val proc = new IntegerColumnProcessor(col());
        proc.processValue("junk value", null);
    }

    @Test(expected = FilterException.class)
    public void process_inval_2() throws Exception {
        val proc = new IntegerColumnProcessor(col());
        proc.processValue(new Object(), null);
    }
}
