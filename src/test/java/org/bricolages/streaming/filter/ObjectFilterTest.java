package org.bricolages.streaming.filter;
import java.util.*;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class ObjectFilterTest {
    OpBuilder builder = new OpBuilder();

    ObjectFilter newFilter() {
        val ops = new ArrayList<Op>();
        ops.add(builder.build(new OperatorDefinition("int", "schema.table", "int_col", "{}")));
        ops.add(builder.build(new OperatorDefinition("bigint", "schema.table", "bigint_col", "{}")));
        ops.add(builder.build(new OperatorDefinition("text", "schema.table", "text_col", "{\"maxByteLength\":10,\"dropIfOverflow\":true}")));
        return new ObjectFilter(null, ops);
    }

    @Test
    public void processStream() throws Exception {
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
        f.processStream(in, bufOut, r, "in");
        bufOut.close();

        assertEquals(expected, out.toString());
        assertEquals(5, r.inputRows);
        assertEquals(3, r.outputRows);
        assertEquals(1, r.errorRows);
    }

    @Test
    public void processRecord() throws Exception {
        val f = newFilter();
        assertEquals("{\"int_col\":1}", f.processRecord("{\"int_col\":1}"));
        assertEquals("{\"int_col\":1,\"bigint_col\":99}", f.processRecord("{\"int_col\":1,\"bigint_col\":99}"));
        assertEquals("{\"int_col\":1}", f.processRecord("{\"int_col\":1,\"bigint_col\":\"b\"}"));
        assertNull(f.processRecord("{}"));
        assertNull(f.processRecord("{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}"));
    }

    @Test(expected=JSONException.class)
    public void processRecord_ParseError() throws Exception {
        val f = newFilter();
        f.processRecord("{");
    }
}
