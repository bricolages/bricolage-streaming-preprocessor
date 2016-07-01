package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class TextOpTest {
    @Test
    public void apply() throws Exception {
        TextOp f = new TextOp(-1, "^00");
        assertEquals("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o", f.apply("00rvvy5o0255d1e392a70217b309fc42c95bd1da2072b0f14o"));
        assertNull(f.apply("  00rvvy5o0255d9e392c70297b909fc40c95bd1ea2072b0f14o"));
        assertNull(f.apply("uuid:92187C83-EA31-4530-8ADD-1C5DAAAAA873"));
        assertNull(f.apply(new Object()));

        f = new TextOp(4, null);
        assertEquals("aaaa", f.apply("aaaa"));
        assertNull(f.apply("aaaaa"));

        f = new TextOp(4, "^00");
        assertEquals("00aa", f.apply("00aa"));
        assertNull(f.apply("aaaa"));
        assertNull(f.apply("00000"));
    }
}
