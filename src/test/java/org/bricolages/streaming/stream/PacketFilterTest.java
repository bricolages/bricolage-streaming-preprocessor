package org.bricolages.streaming.stream;
import org.bricolages.streaming.stream.processor.*;
import org.bricolages.streaming.filter.*;
import java.util.*;
import java.io.*;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class PacketFilterTest {
    OpBuilder builder = new OpBuilder();

    PacketFilter newFilter() {
        val ops = new ArrayList<Op>();
        ops.add(builder.build(new OperatorDefinition("int", "schema.table", "int_col", "{}")));
        ops.add(builder.build(new OperatorDefinition("bigint", "schema.table", "bigint_col", "{}")));
        ops.add(builder.build(new OperatorDefinition("text", "schema.table", "text_col", "{\"maxByteLength\":10,\"dropIfOverflow\":true}")));
        return new PacketFilter(null, ops);
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
        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1}", new FilterResult()));

        val rec = f.processRecord(Record.parse("{\"int_col\":1,\"bigint_col\":2}"), new FilterResult());
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(2L, rec.get("bigint_col"));

        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1,\"bigint_col\":\"b\"}", new FilterResult()));

        assertNull(f.processJSON("{}", new FilterResult()));
        assertNull(f.processJSON("{\"text_col\":\"aaaaaaaaaaaaaaaaaaaaaaaaa\"}", new FilterResult()));
    }

    @Test(expected=JSONException.class)
    public void processRecord_ParseError() throws Exception {
        val f = newFilter();
        f.processJSON("{", new FilterResult());
    }

    @Test
    public void processRecord_processors() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forName("int_col")));
        procs.add(new BigintColumnProcessor(StreamColumn.forName("bigint_col")));
        PacketFilter f = new PacketFilter(null, new ArrayList<Op>(), procs);

        assertTrue(f.useProcessor);
        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1}", new FilterResult()));

        Record rec;
        rec = f.processRecord(Record.parse("{\"int_col\":1,\"bigint_col\":2}"), new FilterResult());
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(2L, rec.get("bigint_col"));

        assertEquals("{\"int_col\":1}", f.processJSON("{\"int_col\":1,\"bigint_col\":\"xxx\"}", new FilterResult()));

        assertNull(f.processJSON("{}", new FilterResult()));
        assertNull(f.processJSON("{\"int_col\":\"str\"}", new FilterResult()));

        rec = f.processRecord(Record.parse("{\"int_col\":1,\"unconsumed\":9}"), new FilterResult());
        assertEquals(2, rec.size());
        assertEquals(1, rec.get("int_col"));
        assertEquals(9, rec.get("unconsumed"));
    }

    @Test
    public void processRecord_processors_rename() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forNames("dest", "src")));
        PacketFilter f = new PacketFilter(null, new ArrayList<Op>(), procs);

        assertEquals("{\"dest\":1}", f.processJSON("{\"src\":1}", new FilterResult()));

        // Do not overwrite
        assertEquals("{\"dest\":1}", f.processJSON("{\"src\":1,\"dest\":2}", new FilterResult()));
    }

    @Test
    public void processRecord_aggregate_object() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new ObjectColumnProcessor(StreamColumn.forName("x"), 20));
        val ops = new ArrayList<Op>();
        ops.add(builder.build(new OperatorDefinition("aggregate", "schema.table", "x", "{\"targetColumns\":\"^q_\", \"aggregatedColumn\":\"x\"}")));
        PacketFilter f = new PacketFilter(null, ops, procs);

        assertTrue(f.useProcessor);
        assertEquals("{\"x\":{\"a\":1,\"b\":2}}", f.processJSON("{\"q_a\":1,\"q_b\":2}", new FilterResult()));
        assertEquals("{\"y\":1}", f.processJSON("{\"y\":1,\"q_a\":\"tooooooooooooooooooooooooooooooooo long\"}", new FilterResult()));
    }

    @Test
    public void processRecord_unknownColumns() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forName("int_col")));
        procs.add(new BigintColumnProcessor(StreamColumn.forName("bigint_col")));
        procs.add(new UnknownColumnProcessor(StreamColumn.forName("unknown_col")));
        PacketFilter f = new PacketFilter(null, new ArrayList<Op>(), procs);

        val record = new Record();
        record.put("int_col", 1);
        record.put("bigint_col", "XXX");   // wrong type
        record.put("unknown_col", 1);
        record.put("a", 1);
        record.put("bbb_bbb", 1);
        record.put("CCC", 1);
        record.put("dddDDD", 1);
        record.put("EE_EE", 1);
        record.put("fff[0]", 1);
        record.put("ggg-ggg", 1);
        record.put("h01", 1);
        record.put("i[a0]", 1);
        record.put("j[3x3]", 1);

        val log = new FilterResult();
        f.processRecord(record, log);

        assertFalse(log.getUnknownColumns().contains("int_col"));
        assertFalse(log.getUnknownColumns().contains("bigint_col"));
        assertFalse(log.getUnknownColumns().contains("unknown_col"));
        assertTrue(log.getUnknownColumns().contains("a"));
        assertTrue(log.getUnknownColumns().contains("bbb_bbb"));
        assertTrue(log.getUnknownColumns().contains("CCC"));
        assertTrue(log.getUnknownColumns().contains("dddDDD"));
        assertTrue(log.getUnknownColumns().contains("EE_EE"));
        assertFalse(log.getUnknownColumns().contains("fff[0]"));
        assertTrue(log.getUnknownColumns().contains("h01"));
        assertFalse(log.getUnknownColumns().contains("i[a0]"));
        assertFalse(log.getUnknownColumns().contains("j[3x3]"));
    }

    @Test
    public void processRecord_unknownColumns_multi() throws Exception {
        val procs = new ArrayList<StreamColumnProcessor>();
        procs.add(new IntegerColumnProcessor(StreamColumn.forName("int_col")));
        procs.add(new BigintColumnProcessor(StreamColumn.forName("bigint_col")));
        PacketFilter f = new PacketFilter(null, new ArrayList<Op>(), procs);

        val record1 = new Record();
        record1.put("a", 1);
        record1.put("b", 1);
        record1.put("c", 1);
        val record2 = new Record();
        record2.put("a", 1);
        record2.put("d", 1);
        record2.put("e", 1);
        record2.put("f", 1);

        val log = new FilterResult();
        f.processRecord(record1, log);
        f.processRecord(record2, log);

        assertEquals(6, log.getUnknownColumns().size());
        assertTrue(log.getUnknownColumns().contains("a"));
        assertTrue(log.getUnknownColumns().contains("b"));
        assertTrue(log.getUnknownColumns().contains("c"));
        assertTrue(log.getUnknownColumns().contains("d"));
        assertTrue(log.getUnknownColumns().contains("e"));
        assertTrue(log.getUnknownColumns().contains("f"));
    }
}
