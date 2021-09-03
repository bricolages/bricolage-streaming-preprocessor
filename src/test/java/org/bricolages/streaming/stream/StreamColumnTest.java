package org.bricolages.streaming.stream;
import org.bricolages.streaming.exception.*;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.*;

@RunWith(SpringJUnit4ClassRunner.class)
public class StreamColumnTest {
    @Test
    public void isValidColumnName() throws Exception {
        assertTrue(StreamColumn.isValidColumnName("column"));
        assertTrue(StreamColumn.isValidColumnName("column22"));
        assertFalse(StreamColumn.isValidColumnName(""));
        assertFalse(StreamColumn.isValidColumnName("column[key]"));
        assertFalse(StreamColumn.isValidColumnName("1column"));
        assertFalse(StreamColumn.isValidColumnName("12345"));
        assertFalse(StreamColumn.isValidColumnName("column-name"));
        assertFalse(StreamColumn.isValidColumnName("column name"));
        assertFalse(StreamColumn.isValidColumnName(" column_name"));
    }
}
