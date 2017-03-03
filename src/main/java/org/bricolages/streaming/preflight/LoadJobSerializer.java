package org.bricolages.streaming.preflight;

import java.io.IOException;
import java.io.OutputStream;
import org.bricolages.streaming.s3.S3ObjectLocation;

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
    
    void serialize(S3ObjectLocation dest, String ctFileName, String destTable) throws IOException {
        val jobDefinition = new LoadJobDefinition(dest.getKey(), ctFileName, destTable);
        val mapper = new ObjectMapper(new YAMLFactory());
        mapper.writeValue(out, jobDefinition);
    }

    @RequiredArgsConstructor
    @Getter
    @JsonNaming(PropertyNamingStrategy.KebabCaseStrategy.class)
    static class LoadJobDefinition {
        @JsonProperty("class")
        private String klass = "load";
        private String srcDs = "redshift-copy-buffer-preflight";
        private final String srcFile;
        private String destDs = "db_data";
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
