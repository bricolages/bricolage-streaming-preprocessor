package org.bricolages.streaming.filter;
import java.util.*;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectFilterTest {
    ObjectFilter newFilter() {
        val ops = new ArrayList<Op>();
        ops.add(Op.build(new OperatorDefinition("int", "schema.table", "int_col", "{}")));
        ops.add(Op.build(new OperatorDefinition("bigint", "schema.table", "bigint_col", "{}")));
        ops.add(Op.build(new OperatorDefinition("text", "schema.table", "text_col", "{\"maxByteLength\":10,\"dropIfOverflow\":true}")));
        return new ObjectFilter(ops);
    }

    @Test
    public void apply() throws Exception {
        val src = "{\"int_col\":1}\n" +
            "{\"int_col\":1,\"bigint_col\":99}\n" +
            "{\n" +
            "{\"int_col\":1,\"bigint_col\":\"b\"}" +
            "\n" +
            "{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}\n";
        val in = new BufferedReader(new StringReader(src));

        val expected = "{\"int_col\":1}\n" +
            "{\"int_col\":1,\"bigint_col\":99}\n" +
            "{\"int_col\":1}\n";

        val f = newFilter();

        val out = new StringWriter();
        val bufOut = new BufferedWriter(out);
        val r = new FilterResult();
        f.apply(in, bufOut, "in", r);
        bufOut.close();

        assertEquals(expected, out.toString());
        assertEquals(5, r.inputRows);
        assertEquals(3, r.outputRows);
        assertEquals(1, r.errorRows);
    }

    @Test
    public void applyString() throws Exception {
        val f = newFilter();
        assertEquals("{\"int_col\":1}", f.applyString("{\"int_col\":1}"));
        assertEquals("{\"int_col\":1,\"bigint_col\":99}", f.applyString("{\"int_col\":1,\"bigint_col\":99}"));
        assertEquals("{\"int_col\":1}", f.applyString("{\"int_col\":1,\"bigint_col\":\"b\"}"));
        assertNull(f.applyString("{}"));
        assertNull(f.applyString("{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}"));
    }

    @Test(expected=JSONException.class)
    public void applyString_parseError() throws Exception {
        val f = newFilter();
        f.applyString("{");
    }
}
