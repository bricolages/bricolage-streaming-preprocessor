package org.bricolages.streaming.stream.processor;
import org.bricolages.streaming.stream.StreamColumn;
import org.bricolages.streaming.filter.FilterException;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class IntegerColumnProcessorTest {
    IntegerColumnProcessor defaultProcessor() {
        return new IntegerColumnProcessor(StreamColumn.forName("x"));
    }

    @Test
    public void process() throws Exception {
        val proc = defaultProcessor();
        assertEquals(Integer.valueOf(1), proc.processValue(Integer.valueOf(1)));
        assertEquals(Integer.valueOf(1), proc.processValue("1"));
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
