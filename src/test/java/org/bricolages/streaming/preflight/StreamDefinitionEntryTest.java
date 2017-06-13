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
    public void load_with_domain() throws Exception {
        val reader1 = new StringReader(String.join("\n", new String[] {
            "log_time: !timestamp",
            "  source_offset: '+00:00'",
            "  target_offset: '+09:00'",
            "jst_time:",
            "  <<: !domain log_time",
            "  name: jst_time",
        }));
        val domains = DomainCollection.load(reader1);
        val reader2 = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - !domain jst_time",
        }));
        val def = StreamDefinitionEntry.load(reader2, domains);
        assertEquals("jst_time", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getEncoding());
    }
}
