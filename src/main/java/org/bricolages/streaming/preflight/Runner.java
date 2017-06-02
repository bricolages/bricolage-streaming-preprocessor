package org.bricolages.streaming.preflight;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.io.FileNotFoundException;
import org.apache.commons.io.FilenameUtils;
import org.bricolages.streaming.Config;
import org.bricolages.streaming.Preprocessor;
import org.bricolages.streaming.SourceLocator;
import org.bricolages.streaming.filter.FilterResult;
import org.bricolages.streaming.filter.ObjectFilterFactory;
import org.bricolages.streaming.filter.ObjectFilter;
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
        try {
            val fileReader = new FileReader(defaultValuesFilePath);
            return PreflightConfig.load(fileReader);
        }
        catch (FileNotFoundException ex) {
            return PreflightConfig.defaultInstance();
        }
    }

    private void saveCreateTableStmt(StreamDefinitionFile streamDefFile, StreamDefinitionEntry streamDef, String fullTableName) throws IOException {
        val createTableStmt = new CreateTableGenerator(streamDef, fullTableName).generate();
        val path = streamDefFile.getCreateTableFilepath();
        System.err.printf("generating table def: %s\n", path.toString());
        try(val writer = new BufferedWriter(new FileWriter(path))) {
            writer.write(createTableStmt);
        }
    }

    private void saveOperatorDefinitions(StreamDefinitionFile streamDefFile, String streamName, List<OperatorDefinition> operators) throws IOException {
        val path = streamDefFile.getOperatorDefinitionsFilepath();
        System.err.printf("generating preproc def: %s\n", path.toString());
        try (val preprocCsvFile = new FileOutputStream(path)) {
            val serializer = new ObjectFilterSerializer(preprocCsvFile);
            serializer.serialize(streamName, operators);
        }
    }

    private void saveLoadJob(StreamDefinitionFile streamDefFile, S3ObjectLocation dest, String fullTableName) throws IOException {
        val path = streamDefFile.getLoadJobFilepath();
        System.err.printf("generating load job: %s\n", path.toString());
        try (val loadJobFile = new FileOutputStream(path)) {
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

    public void run(String streamDefFilename, SourceLocator src, String schemaName, String tableName, boolean generateOnly) throws IOException, S3IOException {
        val preflightConfig = loadPreflightConfig("config/preflight.yml");
        val streamDefFile = new StreamDefinitionFile(streamDefFilename);
        val streamDef = loadStreamDef(streamDefFile);
        streamDef.applyDefaultValues(preflightConfig.getDefaultValues());

        val mapping = mapper.map(src.toString());
        val dest = mapping.getDestLocation();
        val streamName = mapping.getStreamName();

        val generator = new ObjectFilterGenerator(streamDef);
        val operators = generator.generate();
        val filter = factory.compose(operators);
        saveOperatorDefinitions(streamDefFile, streamName, operators);

        val fullTableName = schemaName + "." + tableName;
        saveCreateTableStmt(streamDefFile, streamDef, fullTableName);
        saveLoadJob(streamDefFile, dest, fullTableName);

        if (!generateOnly) {
            applyFilter(filter, src, dest, streamName);
        }
    }

    void applyFilter(ObjectFilter filter, SourceLocator src, S3ObjectLocation dest, String streamName) throws IOException, S3IOException {
        System.err.printf("*** preproc start");
        System.err.printf("preproc source     : %s\n", src.toString());
        System.err.printf("preproc destination: %s\n", dest.toString());
        val result = new FilterResult(src.toString(), dest.urlString());
        preprocessor.applyFilter(filter, src, dest, result, streamName);
        System.err.printf("*** preproc succeeded: in=%d, out=%d, error=%d\n", result.inputRows, result.outputRows, result.errorRows);
    }
}
