package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TextOpTest {
    @Test
    public void apply_pattern() throws Exception {
        val f = new TextOp(null, -1, false, "^00");
        assertEquals("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o", f.applyValue("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o", null));
        assertNull(f.applyValue("  00rvvy5o0255d9e392c70297b909fc40c95bd1ea2072b0f14o", null));
        assertNull(f.applyValue("uuid:92187C83-EA31-4530-8ADD-1C5DAAAAA873", null));
        assertNull(f.applyValue(new Object(), null));
    }

    @Test
    public void apply_length() throws Exception {
        val f = new TextOp(null, 4, false, null);
        assertEquals("aaaa", f.applyValue("aaaa", null));
        assertNull(f.applyValue("aaaaa", null));
    }

    @Test
    public void apply_all() throws Exception {
        val f = new TextOp(null, 4, false, "^00");
        assertEquals("00aa", f.applyValue("00aa", null));
        assertNull(f.applyValue("aaaa", null));
        assertNull(f.applyValue("00000", null));
    }
}
