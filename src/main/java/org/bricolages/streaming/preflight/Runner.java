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
import org.bricolages.streaming.preflight.definition.*;
import org.bricolages.streaming.stream.DataPacketRouter;
import org.bricolages.streaming.filter.*;
import org.bricolages.streaming.locator.*;
import org.bricolages.streaming.exception.*;
import lombok.*;

@RequiredArgsConstructor
public class Runner {
    final ObjectFilterFactory factory;
    final DataPacketRouter router;
    final Config config;

    private DomainCollection loadDomainCollection(String domainCollectionFilePath) throws IOException {
        try {
            val fileReader = new FileReader(domainCollectionFilePath);
            return DomainCollection.load(fileReader);
        } catch(FileNotFoundException ex) {
            return DomainCollection.empty();
        }
    }

    private WellknownColumnCollection loadWellknownCollumnCollection(String wellknownColumnCollectionFilePath, DomainCollection domainCollection) throws IOException {
        try {
            val fileReader = new FileReader(wellknownColumnCollectionFilePath);
            return WellknownColumnCollection.load(fileReader, domainCollection);
        } catch(FileNotFoundException ex) {
            return WellknownColumnCollection.empty();
        }
    }

    private StreamDefinitionEntry loadStreamDef(StreamDefinitionFile streamDefFile, DomainCollection domainCollection, WellknownColumnCollection columnCollection) throws IOException {
        val fileReader = new FileReader(streamDefFile.getFilepath());
        return StreamDefinitionEntry.load(fileReader, domainCollection, columnCollection);
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

    private void saveLoadJob(StreamDefinitionFile streamDefFile, S3ObjectLocator dest, String fullTableName) throws IOException {
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
            if (!FilenameUtils.isExtension(filepath, "yml") && !FilenameUtils.isExtension(filepath, "strdef")) {
                throw new IllegalArgumentException("extension of stream definition file must be 'yml' or 'strdef'");
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

    public void run(String streamDefFilename, S3ObjectLocator src, String schemaName, String tableName, boolean generateOnly) throws IOException, LocatorIOException {
        val streamDefFile = new StreamDefinitionFile(streamDefFilename);
        val domainCollection = loadDomainCollection("config/domains.yml");
        val wellknownColumnCollection = loadWellknownCollumnCollection("config/wellknown_columns.yml", domainCollection);
        val streamDef = loadStreamDef(streamDefFile, domainCollection, wellknownColumnCollection);

        DataPacketRouter.Result route = router.routeWithoutDB(src);
        if (route == null) {
            throw new ConfigError("could not map source URL");
        }
        val dest = route.getDestLocator();
        val streamName = route.getStreamName();

        val generator = new ObjectFilterGenerator(streamDef);
        val operators = generator.generate();
        val filter = factory.compose(operators);
        saveOperatorDefinitions(streamDefFile, streamName, operators);

        val fullTableName = schemaName + "." + tableName;
        saveCreateTableStmt(streamDefFile, streamDef, fullTableName);
        saveLoadJob(streamDefFile, dest, fullTableName);
        if (generateOnly) return;

        System.err.printf("*** preproc start");
        System.err.printf("preproc source     : %s\n", src.toString());
        System.err.printf("preproc destination: %s\n", dest.toString());
        val result = new FilterResult(src.toString(), dest.toString());
        filter.processLocator(src, dest, result, streamName);
        System.err.printf("*** preproc succeeded: in=%d, out=%d, error=%d\n", result.inputRows, result.outputRows, result.errorRows);
    }
}
