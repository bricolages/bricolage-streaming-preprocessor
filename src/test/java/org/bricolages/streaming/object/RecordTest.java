package org.bricolages.streaming.object;
import java.util.*;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RecordTest {
    @Test
    public void parse() throws Exception {
        val rec = Record.parse("{\"a\":1,\"b\":\"str\"}");
        assertEquals(2, rec.size());
        assertEquals(Integer.valueOf(1), rec.get("a"));
        assertEquals("str", rec.get("b"));
    }

    @Test(expected=JSONParseException.class)
    public void parse_parse_error() throws Exception {
        Record.parse("{");
    }

    @Test
    public void unconsumedEntries_1() throws Exception {
        Record rec = new Record();
        rec.put("a", 1);
        rec.put("b", 2);
        rec.put("c", 3);
        rec.consume("a");
        rec.consume("c");
        rec.unconsumedEntries().forEach(ent -> {
            assertEquals("b", ent.getKey());
        });
    }

    @Test
    public void unconsumedEntries_2() throws Exception {
        Record rec = new Record();
        rec.put("a", 1);
        rec.put("b", 2);
        rec.put("c", 3);
        rec.consume("a");
        rec.consume("b");
        rec.consume("c");
        rec.unconsumedEntries().forEach(ent -> {
            fail("should not be called");
        });
    }

    @Test
    public void unconsumedEntries_3() throws Exception {
        Record rec = new Record();
        rec.unconsumedEntries().forEach(ent -> {
            fail("should not be called");
        });
        rec.put("a", 1);
        rec.unconsumedEntries().forEach(ent -> {
            assertEquals("a", ent.getKey());
        });
    }

    @Test
    public void removeAllNullColumns() throws Exception {
        Record rec = new Record();
        rec.put("a", 1);
        rec.put("b", null);
        rec.put("c", 3);
        rec.removeAllNullColumns();
        assertEquals(2, rec.size());
        assertTrue(rec.hasColumn("a"));
        assertFalse(rec.hasColumn("b"));
        assertTrue(rec.hasColumn("c"));
    }
}
