package org.bricolages.streaming.stream.op;
import org.bricolages.streaming.object.Record;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class MetadataOpTest {
    @Test
    public void computeValue() throws Exception {
        val op = new MetadataOp(null, "", false, "");
        assertEquals("table", op.computeValue("streamName", "a.b.schema.table"));
        assertEquals("table", op.computeValue("streamName", "a.b.schema.table/extra"));
    }

    @Test
    public void getStreamName() throws Exception {
        val op = new MetadataOp(null, "", false, "");
        assertEquals("table", op.getStreamName("a.b.schema.table"));
        assertEquals("table", op.getStreamName("a.schema.table"));
        assertEquals("table", op.getStreamName("schema.table"));
    }

    @Test
    public void apply() throws Exception {
        val def = new OperatorDefinition("metadata", null, "col", null);
        val op = new MetadataOp(def, "streamName", false, "a.b.schema.table");
        assertEquals("col", op.getColumnName());
        assertEquals("table", op.value);

        val rec = Record.parse("{}");
        val out = op.apply(rec);
        assertEquals("table", out.get("col"));

        val rec2 = Record.parse("{\"col\":\"EXIST\"}");
        val out2 = op.apply(rec2);
        assertEquals("EXIST", out2.get("col"));
    }

    @Test
    public void apply_overwrite() throws Exception {
        val def = new OperatorDefinition("metadata", null, "col", null);
        val op = new MetadataOp(def, "streamName", true, "a.b.schema.table");

        val rec = Record.parse("{}");
        val out = op.apply(rec);
        assertEquals("table", out.get("col"));

        val rec2 = Record.parse("{\"col\":\"EXIST\"}");
        val out2 = op.apply(rec2);
        assertEquals("table", out2.get("col"));
    }
}
