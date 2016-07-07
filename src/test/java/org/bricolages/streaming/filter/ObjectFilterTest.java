package org.bricolages.streaming.filter;
import java.util.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectFilterTest {
    @Test
    public void applyString() throws Exception {
        val ops = new ArrayList<Op>();
        ops.add(Op.build(new OperatorDefinition(0, "int", "schema.table", "int_col", "{}")));
        ops.add(Op.build(new OperatorDefinition(0, "bigint", "schema.table", "bigint_col", "{}")));
        ops.add(Op.build(new OperatorDefinition(0, "text", "schema.table", "text_col", "{\"maxByteLength\":10,\"dropIfOverflow\":true}")));
        val f = new ObjectFilter(ops);

        assertEquals("{}", f.applyString("{}"));
        assertEquals("{\"int_col\":1}", f.applyString("{\"int_col\":1}"));
        assertEquals("{\"int_col\":1,\"bigint_col\":99}", f.applyString("{\"int_col\":1,\"bigint_col\":99}"));
        assertEquals("{\"int_col\":1}", f.applyString("{\"int_col\":1,\"bigint_col\":\"b\"}"));
        assertNull(f.applyString("{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}"));
    }
}
