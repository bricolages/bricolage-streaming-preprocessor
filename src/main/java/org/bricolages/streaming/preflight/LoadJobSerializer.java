package org.bricolages.streaming.preflight;
import org.bricolages.streaming.locator.S3ObjectLocator;
import java.io.IOException;
import java.io.OutputStream;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.*;

class LoadJobSerializer {
    private final OutputStream out;

    LoadJobSerializer(OutputStream out) {
        this.out = out;
    }
    
    void serialize(S3ObjectLocator dest, String ctFileName, String destTable, String srcDs, String destDs) throws IOException {
        val jobDefinition = new LoadJobDefinition(srcDs, dest.getKey(), destDs, ctFileName, destTable);
        val mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(out, jobDefinition);
    }

    @RequiredArgsConstructor
    @Getter
    @JsonNaming(PropertyNamingStrategy.KebabCaseStrategy.class)
    static class LoadJobDefinition {
        @JsonProperty("class")
        private String klass = "load";
        private final String srcDs;
        private final String srcFile;
        private final String destDs;
        private final String tableDef;
        private final String destTable;
        private String format = "json";
        private boolean drop = true;
        private LoadJobOption options = new LoadJobOption();

        @Getter
        @JsonNaming(PropertyNamingStrategy.KebabCaseStrategy.class)
        static class LoadJobOption {
            private boolean gzip = true;
            private String timeformat = "auto";
            private String dateformat = "auto";
            private boolean truncatecolumns = true;
        }
    }
}
