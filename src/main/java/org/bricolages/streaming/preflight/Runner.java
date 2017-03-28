package org.bricolages.streaming.preflight;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import org.apache.commons.io.FilenameUtils;
import org.bricolages.streaming.Config;
import org.bricolages.streaming.Preprocessor;
import org.bricolages.streaming.filter.FilterResult;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.OperatorDefinition;
import org.bricolages.streaming.preflight.domains.DomainDefaultValues;
import org.bricolages.streaming.s3.ObjectMapper;
import org.bricolages.streaming.s3.S3Agent;
import org.bricolages.streaming.s3.S3IOException;
import org.bricolages.streaming.s3.S3ObjectLocation;

import lombok.*;

@RequiredArgsConstructor
public class Runner {
    final Preprocessor preprocessor;
    final ObjectFilterFactory factory;
    final S3Agent s3;
    final ObjectMapper mapper;
    final Config config;

    private StreamDefinitionEntry loadStreamDef(StreamDefinitionFile streamDefFile) throws IOException {
        val fileReader = new FileReader(streamDefFile.getFilepath());
        return StreamDefinitionEntry.load(fileReader);
    }

    private PreflightConfig loadPreflightConfig(String defaultValuesFilePath) throws IOException {
        val fileReader = new FileReader(defaultValuesFilePath);
        return PreflightConfig.load(fileReader);
    }

    private void saveCreateTableStmt(StreamDefinitionFile streamDefFile, StreamDefinitionEntry streamDef, String fullTableName) throws IOException {
        val createTableStmt = new CreateTableGenerator(streamDef, fullTableName).generate();
        val filepath = streamDefFile.getCreateTableFilepath();
        try(val writer = new BufferedWriter(new FileWriter(filepath))) {
            writer.write(createTableStmt);
        }
    }

    private void saveOperatorDefinitions(StreamDefinitionFile streamDefFile, String streamName, List<OperatorDefinition> operators) throws IOException {
        val filepath = streamDefFile.getOperatorDefinitionsFilepath();
        try (val preprocCsvFile = new FileOutputStream(filepath)) {
            val serializer = new ObjectFilterSerializer(preprocCsvFile);
            serializer.serialize(streamName, operators);
        }
    }

    private void saveLoadJob(StreamDefinitionFile streamDefFile, S3ObjectLocation dest, String fullTableName) throws IOException {
        val filepath = streamDefFile.getLoadJobFilepath();
        try (val loadJobFile = new FileOutputStream(filepath)) {
            new LoadJobSerializer(loadJobFile).serialize(dest, streamDefFile.getCreateTableFilepath(), fullTableName, config.getSrcDs(), config.getDestDs());
        }
    }

    static class StreamDefinitionFile {
        @Getter
        private final String filepath;
        private final String filepathWithoutExt;

        StreamDefinitionFile(String filepath) {
            if (!FilenameUtils.isExtension(filepath, "yml")) {
                throw new IllegalArgumentException("extension of stream definition file must be 'yml'");
            }
            this.filepath = filepath;
            this.filepathWithoutExt = FilenameUtils.removeExtension(filepath);
        }

        String getCreateTableFilepath() {
            return this.filepathWithoutExt + ".ct";
        }

        String getOperatorDefinitionsFilepath() {
            return this.filepathWithoutExt + ".preproc.csv";
        }

        String getLoadJobFilepath() {
            return this.filepathWithoutExt + "_preflight_load_.job";
        }
    }

    public void run(String streamDefFilename, S3ObjectLocation src, String schemaName, String tableName) throws IOException, S3IOException {
        val preflightConfig = loadPreflightConfig("config/preflight.yml");
        val fullTableName = schemaName + "." + tableName;
        val streamDefFile = new StreamDefinitionFile(streamDefFilename);
        val streamDef = loadStreamDef(streamDefFile);
        streamDef.applyDefaultValues(preflightConfig.getDefaultValues());
        System.err.printf("     source: %s\n", src.toString());

        val mapping = mapper.map(src);
        val dest = mapping.getDestLocation();
        System.err.printf("destination: %s\n", dest.toString());

        val generator = new ObjectFilterGenerator(streamDef);
        val operators = generator.generate();
        val filter = factory.compose(operators);
        val result = new FilterResult(src.urlString(), dest.urlString());
        val streamName = mapping.getStreamName();
        preprocessor.applyFilter(filter, src, dest, result, streamName);
        System.err.printf("     result: input rows=%d, output rows=%d, error rows=%d\n", result.inputRows, result.outputRows, result.errorRows);

        saveOperatorDefinitions(streamDefFile, streamName, operators);
        saveCreateTableStmt(streamDefFile, streamDef, fullTableName);
        saveLoadJob(streamDefFile, dest, fullTableName);
    }
}
