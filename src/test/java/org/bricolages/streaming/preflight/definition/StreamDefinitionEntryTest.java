package org.bricolages.streaming.preflight.definition;

import org.junit.Test;
import static org.junit.Assert.*;
import java.io.StringReader;
import lombok.*;

public class StreamDefinitionEntryTest {
    @Test
    public void load() throws Exception {
        val reader = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - time -> jst_time:",
            "      type: timestamp",
            "      encoding: zstd",
        }));
        val def = StreamDefinitionEntry.load(reader, DomainCollection.empty(), WellknownColumnCollection.empty());
        assertEquals("jst_time", def.getColumns().get(0).getName());
        assertEquals("time", def.getColumns().get(0).getOriginalName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getDomain().getEncoding());
    }

    @Test
    public void load_withDomain() throws Exception {
        val reader1 = new StringReader(String.join("\n", new String[] {
            "log_time: !timestamp",
            "  source_offset: '+00:00'",
            "  target_offset: '+09:00'",
        }));
        val domains = DomainCollection.load(reader1);
        val reader2 = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - jst_time: !domain log_time",
        }));
        val def = StreamDefinitionEntry.load(reader2, domains, WellknownColumnCollection.empty());
        assertEquals("jst_time", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getDomain().getEncoding());
        val filter = def.getColumns().get(0).getDomain().getOperatorDefinitionEntries();
        assertEquals(1, filter.size());
        assertEquals("timezone", filter.get(0).getOperatorId());
    }

    @Test
    public void load_withFilterOption() throws Exception {
        val reader = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - item_id:",
            "      <<: !integer",
            "      prepend_filter:",
            "        - op: reject",
            "          params: { type: null }",
            "      append_filter:",
            "        - op: reject",
            "          params: { type: integer, value: 0 }",
        }));
        val def = StreamDefinitionEntry.load(reader, DomainCollection.empty(), WellknownColumnCollection.empty());
        assertEquals("item_id", def.getColumns().get(0).getName());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getDomain().getEncoding());
        val filter = def.getColumns().get(0).getDomain().getOperatorDefinitionEntries();
        assertEquals(3, filter.size());
        assertEquals("reject", filter.get(0).getOperatorId());
        assertEquals("{\"type\":null}", filter.get(0).getParams().toString());
        assertEquals("int", filter.get(1).getOperatorId());
        assertEquals("reject", filter.get(2).getOperatorId());
        assertEquals("{\"type\":\"integer\",\"value\":0}", filter.get(2).getParams().toString());
    }

    @Test
    public void load_complexDefinitions() throws Exception {
        val streamDefReader = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - !column user_id",
            "  - !column jst_time",
            "  - special_item_id: !integer",
            "  - awesome_uuid: !domain uuid",
            "  - hyper_strict_uuid: !domain strict_uuid",    
        }));
        val domainsReader = new StringReader(String.join("\n", new String[] {
            "uuid: !string 36",
            "strict_uuid:",
            "  <<: !domain uuid",
            "  filter:",
            "    - op: text",
            "      params: { pattern: \"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\" }",
            "log_time: !timestamp",
            "  source_offset: '+00:00'",
            "  target_offset: '+09:00'",   
        }));
        val wellknownColumnsReader = new StringReader(String.join("\n", new String[] {
            "jst_time:",
            "  time -> jst_time: !domain log_time",
            "user_id:",
            "  user_id: !integer",
        }));
        val domains = DomainCollection.load(domainsReader);
        val wellknownColumns = WellknownColumnCollection.load(wellknownColumnsReader, domains);
        val def = StreamDefinitionEntry.load(streamDefReader, domains, wellknownColumns);
        assertEquals("user_id",           def.getColumns().get(0).getName());
        assertEquals(null,                def.getColumns().get(0).getOriginalName());
        assertEquals("integer",           def.getColumns().get(0).getDomain().getType());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(0).getDomain().getEncoding());
        assertEquals(1,                   def.getColumns().get(0).getDomain().getOperatorDefinitionEntries().size());
        assertEquals("int",               def.getColumns().get(0).getDomain().getOperatorDefinitionEntries().get(0).getOperatorId());

        assertEquals("jst_time",          def.getColumns().get(1).getName());
        assertEquals("time",              def.getColumns().get(1).getOriginalName());
        assertEquals("timestamp",         def.getColumns().get(1).getDomain().getType());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(1).getDomain().getEncoding());
        assertEquals(1,                   def.getColumns().get(1).getDomain().getOperatorDefinitionEntries().size());
        assertEquals("timezone",          def.getColumns().get(1).getDomain().getOperatorDefinitionEntries().get(0).getOperatorId());
        assertEquals("{\"sourceOffset\":\"+00:00\",\"targetOffset\":\"+09:00\",\"truncate\":false}", def.getColumns().get(1).getDomain().getOperatorDefinitionEntries().get(0).getParams());

        assertEquals("special_item_id",   def.getColumns().get(2).getName());
        assertEquals(null,                def.getColumns().get(2).getOriginalName());
        assertEquals("integer",           def.getColumns().get(2).getDomain().getType());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(2).getDomain().getEncoding());
        assertEquals(1,                   def.getColumns().get(2).getDomain().getOperatorDefinitionEntries().size());
        assertEquals("int",               def.getColumns().get(2).getDomain().getOperatorDefinitionEntries().get(0).getOperatorId());

        assertEquals("awesome_uuid",      def.getColumns().get(3).getName());
        assertEquals(null,                def.getColumns().get(3).getOriginalName());
        assertEquals("varchar(36)",       def.getColumns().get(3).getDomain().getType());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(3).getDomain().getEncoding());
        assertEquals(0,                   def.getColumns().get(3).getDomain().getOperatorDefinitionEntries().size());

        assertEquals("hyper_strict_uuid", def.getColumns().get(4).getName());
        assertEquals(null,                def.getColumns().get(4).getOriginalName());
        assertEquals("varchar(36)",       def.getColumns().get(4).getDomain().getType());
        assertEquals(ColumnEncoding.ZSTD, def.getColumns().get(4).getDomain().getEncoding());
        assertEquals(1,                   def.getColumns().get(4).getDomain().getOperatorDefinitionEntries().size());
        assertEquals("text",              def.getColumns().get(4).getDomain().getOperatorDefinitionEntries().get(0).getOperatorId());
        assertEquals("{\"pattern\":\"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\"}", def.getColumns().get(4).getDomain().getOperatorDefinitionEntries().get(0).getParams());
    }

    @Test(expected = StreamDefinitionLoadingException.class)
    public void load_invalidYaml() throws Exception {
        val reader = new StringReader(String.join("\n", new String[] {
            "columns:",
            "  - incomplete_column:",
            "      type: int",
        }));

        StreamDefinitionEntry.load(reader, DomainCollection.empty(), WellknownColumnCollection.empty());
    }
}
