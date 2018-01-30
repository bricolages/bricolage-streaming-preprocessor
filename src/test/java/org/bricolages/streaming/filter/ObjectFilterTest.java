package org.bricolages.streaming.filter;
import org.bricolages.streaming.stream.*;
import org.bricolages.streaming.stream.processor.*;
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
            "{\"int_col\":1,\"bigint_col\":\"b\"}\n" +
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
        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1}"));

        val rec = f.processRecord(Record.parse("{\"int_col\":1,\"bigint_col\":2}"));
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(2L, rec.get("bigint_col"));

        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1,\"bigint_col\":\"b\"}"));

        assertNull(f.processJSON("{}"));
        assertNull(f.processJSON("{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}"));
    }

    @Test(expected=JSONException.class)
    public void processRecord_ParseError() throws Exception {
        val f = newFilter();
        f.processJSON("{");
    }

    @Test
    public void processRecord_processors() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forName("int_col")));
        procs.add(new BigintColumnProcessor(StreamColumn.forName("bigint_col")));
        ObjectFilter f = new ObjectFilter(null, new ArrayList<Op>(), procs);

        assertTrue(f.useProcessor);
        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1}"));

        Record rec;
        rec = f.processRecord(Record.parse("{\"int_col\":1,\"bigint_col\":2}"));
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(2L, rec.get("bigint_col"));

        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1,\"bigint_col\":\"xxx\"}"));

        assertNull(f.processJSON("{}"));
        assertNull(f.processJSON("{\"int_col\":\"str\"}"));

        rec = f.processRecord(Record.parse("{\"int_col\":1,\"unconsumed\":9}"));
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(9, rec.get("unconsumed"));
    }

    @Test
    public void processRecord_processors_rename() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forNames("dest", "src")));
        ObjectFilter f = new ObjectFilter(null, new ArrayList<Op>(), procs);

        assertEquals("{\"dest\":1}", f.processJSON("{\"src\":1}"));

        // Do not overwrite
        assertEquals("{\"dest\":1}", f.processJSON("{\"src\":1,\"dest\":2}"));
    }
}
