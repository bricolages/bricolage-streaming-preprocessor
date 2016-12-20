package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TextOpTest {
    @Test
    public void build() throws Exception {
        val def = new OperatorDefinition("text", "schema.table", "text_col", "{\"maxByteLength\":10,\"dropIfOverflow\":true}");
        val op = (TextOp)Op.build(def);
        assertEquals("text_col", op.targetColumnName());
        assertEquals(10, op.maxByteLength);
        assertEquals(true, op.dropIfOverflow);
        assertEquals(false, op.createOverflowFlag);
        assertNull(op.pattern);
    }

    @Test
    public void apply_pattern() throws Exception {
        val f = new TextOp(null, -1, false, false, "^00");
        assertEquals("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o", f.applyValue("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o", null));
        assertNull(f.applyValue("  00rvvy5o0255d9e392c70297b909fc40c95bd1ea2072b0f14o", null));
        assertNull(f.applyValue("uuid:92187C83-EA31-4530-8ADD-1C5DAAAAA873", null));
        assertNull(f.applyValue(new Object(), null));
    }

    @Test
    public void apply_dropOverflow() throws Exception {
        val f = new TextOp(null, 4, true, false, null);
        assertEquals("aaaa", f.applyValue("aaaa", null));
        assertNull(f.applyValue("aaaaa", null));
    }

    @Test
    public void apply_overflowFlag() throws Exception {
        val def = new OperatorDefinition("text", "schema.table", "text_col", null);
        val rec = new Record();
        val f = new TextOp(def, 4, false, true, null);

        assertEquals("aaaa", f.applyValue("aaaa", rec));
        assertEquals(false, rec.get("text_col_overflow"));

        assertNull(f.applyValue("\0\0\0\0\0", rec));
        assertEquals(false, rec.get("text_col_overflow"));

        assertEquals("aaaaXX", f.applyValue("aaaaXX", rec));
        assertEquals(true, rec.get("text_col_overflow"));
    }

    @Test
    public void apply_all() throws Exception {
        val f = new TextOp(null, 4, true, false, "^00");
        assertEquals("00aa", f.applyValue("00aa", null));
        assertNull(f.applyValue("aaaa", null));
        assertNull(f.applyValue("00000", null));
    }

    @Test
    public void apply_nothing() throws Exception {
        val f = new TextOp(null, -1, false, false, null);
        assertEquals("abcd\0", f.applyValue("abcd\0", null));
        assertNull(f.applyValue("\0abcd", null));
        assertNull(f.applyValue("\0\0abcd", null));
        assertNull(f.applyValue("\0", null));
        assertNull(f.applyValue("\0\0\0", null));
    }
}
