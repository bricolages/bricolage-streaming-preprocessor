package org.bricolages.streaming.preflight;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.StringReader;
import lombok.*;

public class StreamDefinitionEntryTest {
    @Test
    public void load() throws Exception {
        val reader = new StringReader("columns:\n  - name: jst_time\n    encoding: zstd");
        val def = StreamDefinitionEntry.load(reader, DomainCollection.empty());
        assertEquals("jst_time", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getEncoding());
    }

    @Test
    public void load_withDomain() throws Exception {
        val reader1 = new StringReader(String.join("\n", new String[] {
            "log_time: !timestamp",
            "  source_offset: '+00:00'",
            "  target_offset: '+09:00'",
            "jst_time:",
            "  <<: !domain log_time",
            "  name: jst_time",
            "  original_name: time",
        }));
        val domains = DomainCollection.load(reader1);
        val reader2 = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - !domain jst_time",
        }));
        val def = StreamDefinitionEntry.load(reader2, domains);
        assertEquals("jst_time", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getEncoding());
        assertEquals("time", def.getColumns().get(0).getOriginalName());
        val filter = def.getColumns().get(0).getOperatorDefinitionEntries("jst_time");
        assertEquals(1, filter.size());
        assertEquals("timezone", filter.get(0).getOperatorId());
    }

    @Test
    public void load_withFilterOption() throws Exception {
        val reader = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - <<: !integer",
            "    name: item_id",
            "    prepend_filter:",
            "      - op: reject",
            "        target_column: item_id",
            "        params: { type: null }",
            "    append_filter:",
            "      - op: reject",
            "        target_column: item_id",
            "        params: { type: integer, value: 0 }",
        }));
        val def = StreamDefinitionEntry.load(reader, DomainCollection.empty());
        assertEquals("item_id", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getEncoding());
        assertSame(null, def.getColumns().get(0).getOriginalName());
        val filter = def.getColumns().get(0).getOperatorDefinitionEntries("item_id");
        assertEquals(3, filter.size());
        assertEquals("reject", filter.get(0).getOperatorId());
        assertEquals("{\"type\":null}", filter.get(0).getParams().toString());
        assertEquals("int", filter.get(1).getOperatorId());
        assertEquals("reject", filter.get(2).getOperatorId());
        assertEquals("{\"type\":\"integer\",\"value\":0}", filter.get(2).getParams().toString());
    }
}
