package org.bricolages.streaming.filter;
import org.junit.Test;
import static org.junit.Assert.*;
import lombok.*;

public class RejectOpTest {
    @Test
    public void apply_matched_string() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"string\", \"value\": \"str\"}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":\"str\"}");
        val out = op.apply(rec);
        assertEquals(null, out);
    }

    @Test
    public void apply_not_matched_string() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"string\", \"value\": \"not_matched\"}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":\"str\"}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":\"str\"}", out.serialize());
    }

    @Test
    public void apply_matched_integer() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"integer\", \"value\": 1}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":1}");
        val out = op.apply(rec);
        assertEquals(null, out);
    }

    @Test
    public void apply_not_matched_integer() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"integer\", \"value\": 2}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":1}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":1}", out.serialize());
    }

    @Test
    public void apply_matched_null() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"null\"}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":null}");
        val out = op.apply(rec);
        assertEquals(null, out);
    }

    @Test
    public void apply_not_matched_non_null() throws Exception {
        val def = new OperatorDefinition("reject", "schema.table", "hoge", "{\"type\": \"null\"}");
        val op = (RejectOp)Op.build(def);
        val rec = Record.parse("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":\"non_null\"}");
        val out = op.apply(rec);
        assertEquals("{\"a\":1,\"b\":2,\"c\":3,\"hoge\":\"non_null\"}", out.serialize());
    }
}
